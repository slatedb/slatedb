pub(crate) struct WriteOnceRegister<T: Clone> {
    pub(crate) rx: tokio::sync::watch::Receiver<Option<T>>,
    pub(crate) tx: tokio::sync::watch::Sender<Option<T>>
}

impl <T: Clone> WriteOnceRegister<T> {
    pub(crate) fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(None);
        Self{
            rx,
            tx,
        }
    }

    pub(crate) fn write(&self, val: T) {
        self.tx.send_if_modified(|v| {
            if v.is_some() {
                return false;
            }
            v.replace(val);
            true
        });
    }

    pub(crate) fn read(&self) -> Option<T> {
        self.rx.borrow().clone()
    }

    pub(crate) async fn await_value(&self) -> T {
        let mut rx = self.rx.clone();
        loop {
            if let Some(maybe_value) = rx.borrow_and_update().clone() {
                return maybe_value;
            }
            rx.changed().await.expect("watch channel closed")
        }
    }
}
