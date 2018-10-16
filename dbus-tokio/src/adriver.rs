use std::sync::Mutex;
use std::sync::RwLock;
use mio::{self, unix, Ready};
use mio::unix::UnixReady;
use std::io;
use dbus::{Connection, ConnMsgs, Watch, WatchEvent, Message, MessageType, Error as DBusError};
use futures::{Async, Future, Stream, Poll};
use futures::sync::{oneshot, mpsc};
use futures::future;
use tokio;
use tokio::prelude::*;
use tokio::reactor::PollEvented2;
use tokio::runtime::Runtime;
use std::borrow::BorrowMut;
use std::borrow::Borrow;
use std::rc::Rc;
use std::sync::Arc;
use std::os::raw::c_uint;
use std::collections::HashMap;
use std::os::unix::io::RawFd;

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
    pub quit_tx: Option<Arc<oneshot::Sender<()>>>,
    callmap: MCallMap,
    msgstream: MStream,
}

impl AConnection {
    /// Create an AConnection, which spawns a task on the core.
    ///
    /// The task handles incoming messages, and continues to do so until the
    /// AConnection is dropped.
    pub fn new(c: Arc<Connection>) -> io::Result<(AConnection, ADriver)>  {
        let map: MCallMap = Default::default();
        let istream: MStream = Default::default();
        let (quit_tx, quit_rx) = oneshot::channel();

      let mut d = ADriver {
            conn: c.clone(),
            fds: HashMap::new(),
            quit:  quit_rx,
            callmap: map.clone(),
            msgstream: istream.clone(),
        };

        c.set_watch_callback(Box::new(|_| unimplemented!("Watch handling is very rare and not implemented yet")));


        let mut i = AConnection {
            conn: c,
            quit_tx: Some(Arc::new(quit_tx)),
            callmap: map,
            msgstream: istream,
        };


        for w in i.conn.watch_fds() {
            d.modify_watch(w, false)?;
        }

        Ok((i, d))
    }

    /// Sends a method call message, and returns a Future for the method return.
    pub fn method_call(&self, m: Message) -> Result<AMethodCall, &'static str> {
        let r = self.conn.send(m).map_err(|_| "D-Bus send error")?;
        let (tx, rx) = oneshot::channel();
        {
            let mut map = self.callmap.lock().unwrap();
            map.insert(r, tx); // TODO: error check for duplicate entries. Should not happen, but if it does...
        }
        let mc = AMethodCall { serial: r, callmap: self.callmap.clone(), inner: rx };
        Ok(mc)
    }
}

impl Drop for AConnection {
    fn drop(&mut self) {
        debug!("Dropping AConnection");
        if let Ok(x) = Arc::try_unwrap(self.quit_tx.take().unwrap()) {
            debug!("AConnection telling ADriver to quit");
            let _ = x.send(());
        }
        self.conn.set_watch_callback(Box::new(|_| {}));
    }
}

#[derive(Debug)]
// Internal struct; this is the future spawned on the core.
pub struct ADriver {
    conn: Arc<Connection>,
    fds: HashMap<RawFd, PollEvented2<AWatch>>,
    quit: oneshot::Receiver<()>,
    callmap: MCallMap,
    msgstream: MStream,
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
                    return Ok(())
                };
            }
            self.fds.remove(&w.fd());

            let z = PollEvented2::new(AWatch(w));

            if poll_now && z.get_ref().0.readable() { z.clear_read_ready(Ready::readable())?; };
            if poll_now && z.get_ref().0.writable() { z.clear_write_ready()?; };

            self.fds.insert(w.fd(), z);
        }
        Ok(())
    }

    fn send_stream(&self, m: Message) {
        let mut msgstream = self.msgstream.lock().unwrap();
        msgstream.as_ref().map(|ref z| { z.unbounded_send(m).unwrap() });
    }

    fn handle_msgs(&mut self) {
        let msgs = ConnMsgs { conn: self.conn.clone(), timeout_ms: None };
        for m in msgs {
            debug!("handle_msgs: {:?}", m);
            if m.msg_type() == MessageType::MethodReturn {
                let mut map = self.callmap.lock().unwrap();
                let serial = m.get_reply_serial().unwrap();
                let r = map.remove(&serial);
                debug!("Serial {:?}, found: {:?}", serial, r.is_some());
                if let Some(r) = r { r.send(m).unwrap(); }
                else { self.send_stream(m) }
            } else { self.send_stream(m) }
        }
    }
}

impl Future for ADriver {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        let q = self.quit.poll();
        if q != Ok(Async::NotReady) { return Ok(Async::Ready(())); }

        for w in self.fds.values() {
            let mut mask = UnixReady::hup() | UnixReady::error();
            if w.get_ref().0.readable() { mask = mask | Ready::readable().into(); }
            if w.get_ref().0.writable() { mask = mask | Ready::writable().into(); }
            let prr = w.poll_read_ready(*mask).map_err(|_| ())?;
            debug!("D-Bus i/o poll read ready: {:?} is {:?}", w.get_ref().0.fd(), prr);
            let pwr = w.poll_write_ready().map_err(|_| ())?;
            debug!("D-Bus i/o poll write ready: {:?} is {:?}", w.get_ref().0.fd(), pwr);
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
            let flags =
                if ur.is_readable() { WatchEvent::Readable as c_uint } else { 0 } +
                if ur.is_writable() { WatchEvent::Writable as c_uint } else { 0 } +
                if ur.is_hup() { WatchEvent::Hangup as c_uint } else { 0 } +
                if ur.is_error() { WatchEvent::Error as c_uint } else { 0 };
            debug!("D-Bus i/o unix ready: {:?} is {:?}", w.get_ref().0.fd(), ur);
            self.conn.watch_handle(w.get_ref().0.fd(), flags);
            if ur.is_readable() { w.clear_read_ready(Ready::readable()).map_err(|_| ())?; };
            if ur.is_writable() { w.clear_write_ready().map_err(|_| ())?; };
        };
        self.handle_msgs();
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
struct AWatch(Watch);

impl mio::Evented for AWatch {
    fn register(&self,
                poll: &mio::Poll,
                token: mio::Token,
                mut interest: mio::Ready,
                mut opts: mio::PollOpt) -> io::Result<()>
    {
        if !self.0.readable() { interest.remove(mio::Ready::readable()) };
        if !self.0.writable() { interest.remove(mio::Ready::writable()) };
        opts.remove(mio::PollOpt::edge());
        opts.insert(mio::PollOpt::level());
        unix::EventedFd(&self.0.fd()).register(poll, token, interest, opts)
    }

    fn reregister(&self,
                  poll: &mio::Poll,
                  token: mio::Token,
                  mut interest: mio::Ready,
                  mut opts: mio::PollOpt) -> io::Result<()>
    {
        if !self.0.readable() { interest.remove(mio::Ready::readable()) };
        if !self.0.writable() { interest.remove(mio::Ready::writable()) };
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
        let x = self.inner.poll().map_err(|_| DBusError::new_custom("org.freedesktop.DBus.Failed", "Tokio cancelled future"))?;
        if let Async::Ready(mut m) = x {
            m.as_result()?;
            Ok(Async::Ready(m))
        } else { Ok(Async::NotReady) }
    }
}

impl Drop for AMethodCall {
    fn drop(&mut self) {
        let mut map = self.callmap.lock().unwrap();
        map.remove(&self.serial);
    }
}

#[derive(Debug)]
/// A Stream of incoming messages.
///
/// Messages already processed (method returns for AMethodCall)
/// are already consumed and will not be present in the stream.
pub struct AMessageStream {
    driver: ADriver,
    inner: mpsc::UnboundedReceiver<Message>,
    quit: Option<Arc<oneshot::Sender<()>>>,
    //stream: MStream,
}

impl AMessageStream {
    /// Returns a stream of incoming messages.
    ///
    /// Creating more than one stream for the same AConnection is not supported; this function will
    /// fail with an error if you try. Drop the first stream if you need to create a second one.
    pub fn messages(driver: ADriver, quit_tx: Option<Arc<oneshot::Sender<()>>>) -> Result<AMessageStream, &'static str> {
        let (tx, rx) = mpsc::unbounded();
        {
            let mut i = driver.msgstream.lock().unwrap();
            if i.is_some() { return Err("Another instance of AMessageStream already exists"); }
            *i = Some(tx);
        }
        Ok(AMessageStream { driver: driver, inner: rx, quit: quit_tx })
    }
}

impl Stream for AMessageStream {
    type Item = Message;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        debug!("Polling Driver");
        self.driver.poll()?;
        debug!("Polling message stream");
        let r = self.inner.poll();
        debug!("msgstream found {:?}", r);
        r
    }
}

impl Drop for AMessageStream {
    fn drop(&mut self) {
        {
            let mut stream = self.driver.msgstream.lock().unwrap();
            *stream = None;
        }
        debug!("Dropping AMessageStream");
        if let Ok(x) = Arc::try_unwrap(self.quit.take().unwrap()) {
            debug!("AMessageStream telling ADriver to quit");
            let _ = x.send(());
        }
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
    assert_eq!(firstsig.unwrap().msg_type(), ::dbus::MessageType::Signal);*/
}

