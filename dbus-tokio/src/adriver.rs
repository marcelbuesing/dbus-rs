use dbus::{ConnMsgs, Connection, Error as DBusError, Message, MessageType, Watch, WatchEvent};
use futures::sync::{mpsc, oneshot};
use futures::{Async, Future, Poll, Stream};
use mio::unix::UnixReady;
use mio::{self, unix, Ready};
use std::collections::HashMap;
use std::io;
use std::os::raw::c_uint;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::reactor::PollEvented2;

type MCallMap = Arc<Mutex<HashMap<u32, oneshot::Sender<Message>>>>;

// Message stream
type MStream = Arc<Mutex<Option<mpsc::UnboundedSender<Message>>>>;

#[derive(Debug)]
/// A Tokio enabled D-Bus connection.
///
/// While an AConnection exists, it will consume all incoming messages.
/// Creating more than one AConnection for the same Connection is not recommended.
pub struct AConnection {
    conn: Arc<Connection>,
    callmap: MCallMap,
    msgstream: MStream,
}

impl AConnection {
    /// Create an AConnection, which spawns a task on the core.
    ///
    /// The task handles incoming messages, and continues to do so until the
    /// AConnection is dropped.
    pub fn new(c: Arc<Connection>) -> io::Result<AConnection> {
        let map: MCallMap = Default::default();
        let istream: MStream = Default::default();

        c.set_watch_callback(Box::new(|_| {
            unimplemented!("Watch handling is very rare and not implemented yet")
        }));

        let i = AConnection {
            conn: c,
            callmap: map,
            msgstream: istream,
        };

        Ok(i)
    }

    /// Returns a stream of incoming messages.
    ///
    /// Creating more than one stream for the same AConnection is not supported; this function will
    /// fail with an error if you try. Drop the first stream if you need to create a second one.
    pub fn messages(self) -> Result<ADriver, &'static str> {
        let mut driver = ADriver {
            conn: self.conn.clone(),
            fds: HashMap::new(),
            callmap: self.callmap.clone(),
        };

        for w in self.conn.watch_fds() {
            driver.modify_watch(w, false).map_err(|e| {
                error!("Failed to modify watches: {:?}", e);
                "Failed to modify watches"
            })?;
        }

        Ok(driver)
    }

    /// Sends a method call message, and returns a Future for the method return.
    pub fn method_call(&self, m: Message) -> Result<AMethodCall, &'static str> {
        let r = self.conn.send(m).map_err(|_| "D-Bus send error")?;
        let (tx, rx) = oneshot::channel();
        {
            let mut map = self.callmap.lock().unwrap();
            map.insert(r, tx); // TODO: error check for duplicate entries. Should not happen, but if it does...
        }
        let mc = AMethodCall {
            serial: r,
            callmap: self.callmap.clone(),
            inner: rx,
        };
        Ok(mc)
    }
}

impl Drop for AConnection {
    fn drop(&mut self) {
        debug!("Dropping AConnection");
        self.conn.set_watch_callback(Box::new(|_| {}));
    }
}

#[derive(Debug)]
// Internal struct; this is the future spawned on the core.
pub struct ADriver {
    conn: Arc<Connection>,
    fds: HashMap<RawFd, PollEvented2<AWatch>>,
    callmap: MCallMap,
}

impl ADriver {
    fn modify_watch(&mut self, w: Watch, poll_now: bool) -> io::Result<()> {
        debug!("Modify_watch: {:?}, poll_now: {:?}", w, poll_now);
        if !w.readable() && !w.writable() {
            self.fds.remove(&w.fd());
        } else {
            if let Some(evented) = self.fds.get(&w.fd()) {
                let ww = evented.get_ref().0;
                if ww.readable() == w.readable() && ww.writable() == w.writable() {
                    return Ok(());
                };
            }
            self.fds.remove(&w.fd());

            let z = PollEvented2::new(AWatch(w));

            if poll_now && z.get_ref().0.readable() {
                z.clear_read_ready(Ready::readable())?;
            };
            if poll_now && z.get_ref().0.writable() {
                z.clear_write_ready()?;
            };

            self.fds.insert(w.fd(), z);
        }
        Ok(())
    }

    /*fn handle_msgs(&mut self) {
    let msgs = ConnMsgs { conn: self.conn.clone(), timeout_ms: None };
    for m in msgs {
        debug!("handle_msgs: {:?}", m);
        /*if m.msg_type() == MessageType::MethodReturn {
            let mut map = self.callmap.lock().unwrap();
            let serial = m.get_reply_serial().unwrap();
            let r = map.remove(&serial);
            debug!("Serial {:?}, found: {:?}", serial, r.is_some());
            if let Some(r) = r { r.send(m).unwrap(); }
            else { self.send_stream(m) }
        }*/
    }
    }*/
}

impl Stream for ADriver {
    type Item = Message;
    type Error = ();

    fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
        for w in self.fds.values() {
            let mut mask = UnixReady::hup() | UnixReady::error();
            if w.get_ref().0.readable() {
                mask = mask | Ready::readable().into();
            }
            //if w.get_ref().0.writable() { mask = mask | Ready::writable().into(); }
            let prr = w.poll_read_ready(*mask).map_err(|_| ())?;
            debug!(
                "D-Bus i/o poll read ready: {:?} is {:?}",
                w.get_ref().0.fd(),
                prr
            );
            let pwr = w.poll_write_ready().map_err(|_| ())?;
            debug!(
                "D-Bus i/o poll write ready: {:?} is {:?}",
                w.get_ref().0.fd(),
                pwr
            );
            let ur = if let Async::Ready(t) = prr {
                UnixReady::from(t) | if let Async::Ready(wt) = pwr {
                    UnixReady::from(wt)
                } else {
                    Ready::empty().into()
                }
            } else {
                if let Async::Ready(wt) = pwr {
                    UnixReady::from(wt)
                } else {
                    continue;
                }
            };
            let flags = if ur.is_readable() {
                WatchEvent::Readable as c_uint
            } else {
                0
            } + if ur.is_writable() {
                WatchEvent::Writable as c_uint
            } else {
                0
            } + if ur.is_hup() {
                WatchEvent::Hangup as c_uint
            } else {
                0
            } + if ur.is_error() {
                WatchEvent::Error as c_uint
            } else {
                0
            };

            debug!("D-Bus i/o unix ready: {:?} is {:?}", w.get_ref().0.fd(), ur);
            self.conn.watch_handle(w.get_ref().0.fd(), flags);
            if ur.is_readable() {
                let mut msgs = ConnMsgs { conn: self.conn.clone(), timeout_ms: None };
                if let Some(msg) = msgs.next() {
                    return Ok(Async::Ready(Some(msg)));
                } else {
                    w.clear_read_ready(Ready::readable()).map_err(|_| ())?;
                };
            };

            if ur.is_writable() {
                w.clear_write_ready().map_err(|_| ())?;
            };
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
struct AWatch(Watch);

impl mio::Evented for AWatch {
    fn register(
        &self,
        poll: &mio::Poll,
        token: mio::Token,
        mut interest: mio::Ready,
        mut opts: mio::PollOpt,
    ) -> io::Result<()> {
        if !self.0.readable() {
            interest.remove(mio::Ready::readable())
        };
        if !self.0.writable() {
            interest.remove(mio::Ready::writable())
        };
        opts.remove(mio::PollOpt::edge());
        opts.insert(mio::PollOpt::level());
        unix::EventedFd(&self.0.fd()).register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &mio::Poll,
        token: mio::Token,
        mut interest: mio::Ready,
        mut opts: mio::PollOpt,
    ) -> io::Result<()> {
        if !self.0.readable() {
            interest.remove(mio::Ready::readable())
        };
        if !self.0.writable() {
            interest.remove(mio::Ready::writable())
        };
        opts.remove(mio::PollOpt::edge());
        opts.insert(mio::PollOpt::level());
        unix::EventedFd(&self.0.fd()).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        unix::EventedFd(&self.0.fd()).deregister(poll)
    }
}

#[derive(Debug)]
/// A Future that resolves when a method call is replied to.
pub struct AMethodCall {
    serial: u32,
    callmap: MCallMap,
    inner: oneshot::Receiver<Message>,
}

impl Future for AMethodCall {
    type Item = Message;
    type Error = DBusError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let x = self.inner.poll().map_err(|_| {
            DBusError::new_custom("org.freedesktop.DBus.Failed", "Tokio cancelled future")
        })?;
        if let Async::Ready(mut m) = x {
            m.as_result()?;
            Ok(Async::Ready(m))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl Drop for AMethodCall {
    fn drop(&mut self) {
        let mut map = self.callmap.lock().unwrap();
        map.remove(&self.serial);
    }
}

#[test]
fn aconnection_test() {
    let conn = Arc::new(Connection::get_private(::dbus::BusType::Session).unwrap());

    let aconn = AConnection::new(conn.clone());

    let mut rt = Runtime::new().unwrap();

    /* let m = ::dbus::Message::new_method_call("org.freedesktop.DBus", "/", "org.freedesktop.DBus", "ListNames").unwrap();
    let reply = rt.block_on(aconn.method_call(m).unwrap()).unwrap();
    let z: Vec<&str> = reply.get1().unwrap();
    println!("got reply: {:?}", z);
    assert!(z.iter().any(|v| *v == "org.freedesktop.DBus"));*/
}

#[test]
fn astream_test() {
    let conn = Connection::get_private(::dbus::BusType::Session).unwrap();

    /*let aconn = AConnection::new(Arc::new(Mutex::new(conn)));
    let f = aconn.map_err(|_| ()).and_then(|conn| {
        let f1 = conn.messages().unwrap().for_each(|msg| {
            println!("first signal was: {:?}", msg);
            Ok(())
        });
        tokio::spawn(f1);
        Ok(())
    });*/

    //let mut rt = Runtime::new().unwrap();
    //tokio::run(f);
    /*let items: AMessageStream = rt.block_on().unwrap();
    let signals = items.filter_map(|m| if m.msg_type() == ::dbus::MessageType::Signal { Some(m) } else { None });
    let firstsig = rt.block_on(signals.into_future()).map(|(x, _)| x).map_err(|(x, _)| x).unwrap();
    println!("first signal was: {:?}", firstsig);
    assert_eq!(firstsig.unwrap().msg_type(), ::dbus::MessageType::Signal);*/}
