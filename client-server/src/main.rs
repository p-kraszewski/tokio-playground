use tokio::{
    select,
    signal::unix::{signal, Signal, SignalKind},
    sync::{mpsc, oneshot},
    task, time,
};

#[derive(Clone)]
struct RPC<Request, Reply> {
    server: mpsc::Sender<Callback<Request, Reply>>,
}

impl<Request, Reply> RPC<Request, Reply>
where
    Request: std::fmt::Debug + 'static,
    Reply: std::fmt::Debug + 'static,
{
    fn new(buffer: usize) -> (Self, mpsc::Receiver<Callback<Request, Reply>>) {
        let (stx, srx) = mpsc::channel::<Callback<Request, Reply>>(buffer);

        (Self { server: stx }, srx)
    }

    async fn request(&self, req: Request) -> Result<Reply, Box<dyn std::error::Error>> {
        let (tx, rx) = oneshot::channel::<Reply>();
        let r = Callback {
            data:  req,
            reply: tx,
        };
        self.server.send(r).await?;
        let ans = rx.await?;
        Ok(ans)
    }
}

#[derive(Debug)]
struct Callback<Request, Reply> {
    data:  Request,
    reply: oneshot::Sender<Reply>,
}

type MyCallback = Callback<i32, i32>;

struct Processor {
    srv:     mpsc::Receiver<MyCallback>,
    timer:   time::Interval,
    signals: Signal,
}

impl Processor {
    async fn ctrl_c(&self) {
        println!("S ^C :D  !");
    }

    async fn tick(&self) {
        println!("S Tick!");
    }

    async fn process(&self, msg: Option<Callback<i32, i32>>) -> bool {
        match msg {
            None => {
                println!("S Close");
                true
            }
            Some(i) => {
                let res = i.data * 2;
                println!("S got = {}, reply = {}", i.data, res);
                i.reply.send(res).unwrap();
                false
            }
        }
    }

    async fn serve(mut self) {
        loop {
            select! {
               _ = self.signals.recv() => { self.ctrl_c().await; }
               _ = self.timer.tick() => { self.tick().await; }
               rcv = self.srv.recv() => { if self.process(rcv).await { break; } }
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let t1;

    {
        let (rpc, srv) = RPC::<i32, i32>::new(100);

        let handler = Processor {
            srv,
            timer: time::interval(time::Duration::from_millis(300)),
            signals: signal(SignalKind::interrupt())?,
        };

        t1 = task::spawn(handler.serve());

        for i in 0 .. 10 {
            println!("C sent = {}", i);
            let rep = rpc.request(i).await?;
            time::sleep(time::Duration::from_secs(2)).await;
            println!("C got = {}", rep);
        }
    }

    t1.await?;
    Ok(())
}
