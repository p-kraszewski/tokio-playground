use tokio::{
    select,
    signal::unix::{signal, SignalKind},
    sync::{broadcast, oneshot},
    task, time,
};

#[derive(Clone)]
struct Dispatcher<Request, Reply> {
    server: async_channel::Sender<Message<Request, Reply>>,
}

impl<Request, Reply> Dispatcher<Request, Reply>
where
    Request: std::fmt::Debug + 'static,
    Reply: std::fmt::Debug + 'static,
{
    fn new(buffer: usize) -> (Self, async_channel::Receiver<Message<Request, Reply>>) {
        let (stx, srx) = async_channel::bounded::<Message<Request, Reply>>(buffer);

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
    id:      usize,
    done:    bool,
    queue:   async_channel::Receiver<MyMessage>,
    timer:   broadcast::Receiver<u32>,
    signals: broadcast::Receiver<()>,
}

impl MyProcessor {
    async fn ctrl_c(&self) {
        println!("   S{} Seen ^C!", self.id);
    }

    async fn tick(&self, t: u32) {
        println!("   S{} Tick {}!", self.id, t);
    }

    async fn process(&mut self, msg: Option<Message<i32, i32>>) {
        match msg {
            None => {
                println!("   S{} Close", self.id);
                self.done = true
            }
            Some(i) => {
                let res = i.data * 2;
                println!("   S{} got = {}, reply = {}", self.id, i.data, res);
                i.reply.send(res).unwrap();
            }
        }
    }

    async fn serve(mut self) {
        while !self.done {
            select! {
               _ = self.signals.recv() => { self.ctrl_c().await; }
               t = self.timer.recv() => { self.tick(t.unwrap()).await; }
               msg = self.queue.recv() => { self.process(msg.ok()).await; }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let mut handles = Vec::new();

    {
        let (dispatcher, req_queue) = Dispatcher::<i32, i32>::new(100);
        let (time_tx, _) = broadcast::channel(10);
        let (signal_tx, _) = broadcast::channel(10);

        let time_tx2 = time_tx.clone();

        handles.push(tokio::spawn(async move {
            let mut ticker = time::interval(time::Duration::from_millis(300));

            for t in 0 .. {
                let _ = ticker.tick().await;
                if time_tx2.send(t).is_err() {
                    println!("TIM cancelled!");
                    break;
                };
            }
            println!("TIM ended!");
        }));

        let signal_tx2 = signal_tx.clone();

        handles.push(tokio::spawn(async move {
            let mut handler = signal(SignalKind::interrupt()).unwrap();

            while (handler.recv().await).is_some() {
                if signal_tx2.send(()).is_err() {
                    println!("SIG cancelled!");
                    break;
                };
            }
            println!("SIG ended!");
        }));

        for i in 0 .. 3 {
            let processor = MyProcessor {
                id:      i,
                done:    false,
                queue:   req_queue.clone(),
                timer:   time_tx.subscribe(),
                signals: signal_tx.subscribe(),
            };
            handles.push(task::spawn(processor.serve()));
        }

        for i in 0 .. 4 {
            println!("C sent = {}", i);
            let rep = dispatcher.send(i).await?;
            println!("C got = {}", rep);
            time::sleep(time::Duration::from_secs(1)).await;
        }
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}
