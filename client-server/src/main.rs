use tokio::{
    select,
    signal::unix::{signal, Signal, SignalKind},
    sync::{mpsc, oneshot},
    task, time,
};

#[derive(Clone)]
struct Dispatcher<Request, Reply> {
    server: mpsc::Sender<Message<Request, Reply>>,
}

impl<Request, Reply> Dispatcher<Request, Reply>
where
    Request: std::fmt::Debug + 'static,
    Reply: std::fmt::Debug + 'static,
{
    fn new(buffer: usize) -> (Self, mpsc::Receiver<Message<Request, Reply>>) {
        let (stx, srx) = mpsc::channel::<Message<Request, Reply>>(buffer);

        (Self { server: stx }, srx)
    }

    async fn send(&self, req: Request) -> Result<Reply, Box<dyn std::error::Error>> {
        let (tx, rx) = oneshot::channel::<Reply>();
        let r = Message {
            data:  req,
            reply: tx,
        };
        self.server.send(r).await?;
        let ans = rx.await?;
        Ok(ans)
    }
}

#[derive(Debug)]
struct Message<Request, Reply> {
    data:  Request,
    reply: oneshot::Sender<Reply>,
}

type MyMessage = Message<i32, i32>;

struct MyProcessor {
    done:    bool,
    queue:   mpsc::Receiver<MyMessage>,
    timer:   time::Interval,
    signals: Signal,
}

impl MyProcessor {
    async fn ctrl_c(&self) {
        println!("   S Seen ^C!");
    }

    async fn tick(&self) {
        println!("   S Tick!");
    }

    async fn process(&mut self, msg: Option<Message<i32, i32>>) {
        match msg {
            None => {
                println!("   S Close");
                self.done = true
            }
            Some(i) => {
                let res = i.data * 2;
                println!("   S got = {}, reply = {}", i.data, res);
                i.reply.send(res).unwrap();
            }
        }
    }

    async fn serve(mut self) {
        while !self.done {
            select! {
               _ = self.signals.recv() => { self.ctrl_c().await; }
               _ = self.timer.tick() => { self.tick().await; }
               msg = self.queue.recv() => { self.process(msg).await; }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let t1;

    {
        let (dispatcher, req_queue) = Dispatcher::<i32, i32>::new(100);

        let processor = MyProcessor {
            done:    false,
            queue:   req_queue,
            timer:   time::interval(time::Duration::from_millis(300)),
            signals: signal(SignalKind::interrupt())?,
        };

        t1 = task::spawn(processor.serve());

        for i in 0 .. 10 {
            println!("C sent = {}", i);
            let rep = dispatcher.send(i).await?;
            println!("C got = {}", rep);
            time::sleep(time::Duration::from_secs(2)).await;
        }
    }

    t1.await?;
    Ok(())
}
