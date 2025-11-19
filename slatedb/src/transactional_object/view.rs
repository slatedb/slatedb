use crate::transactional_object::{DirtyObject, MonotonicId, TransactionalObject, TransactionalObjectError};

pub(crate) trait DirtyView<T, Id: Copy = MonotonicId> {
    fn merge(&mut self, other: DirtyObject<T, Id>);

    fn value(&self) -> DirtyObject<T, Id>;
}

pub(crate) struct DirtyViewManager<T, V, O, Id = MonotonicId>
where
    T: Clone,
    Id: Copy,
    V: DirtyView<T, Id>,
    O: TransactionalObject<T, Id>,
{
    view: V,
    object: O,
    _marker: std::marker::PhantomData<(T, Id)>,
}

impl <T: Clone, V: DirtyView<T, Id>, O: TransactionalObject<T, Id>, Id: Copy> DirtyViewManager<T, V, O, Id> {
    fn new(view: V, object: O) -> Self {
        Self {
            view,
            object,
            _marker: Default::default(),
        }
    }

    fn view(&self) -> &V {
        &self.view
    }

    fn view_mut(&mut self) -> &mut V {
        &mut self.view
    }

    async fn push(&mut self) -> Result<(), TransactionalObjectError> {
        loop {
            match self.object.update(self.view.value()).await {
                Ok(_) => return Ok(()),
                Err(TransactionalObjectError::ObjectVersionExists) => {
                    self.object.refresh().await?;
                    self.view.merge(self.object.prepare_dirty()?)
                },
                Err(err) => return Err(err)
            }
        }
    }

    async fn pull(&mut self) -> Result<(), TransactionalObjectError> {
        self.object.refresh().await?;
        self.view.merge(self.object.prepare_dirty()?);
        Ok(())
    }
}