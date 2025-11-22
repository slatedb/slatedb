use crate::transactional_object::{
    DirtyObject, MonotonicId, TransactionalObject, TransactionalObjectError,
};
use std::error::Error;

pub(crate) trait LocalView<T, Id: Copy = MonotonicId> {
    fn merge(&mut self, other: DirtyObject<T, Id>);

    fn value(&self) -> &DirtyObject<T, Id>;
}

pub(crate) struct LocalViewManager<T, V, O, Id = MonotonicId>
where
    T: Clone,
    Id: Copy,
    V: LocalView<T, Id>,
    O: TransactionalObject<T, Id>,
{
    view: V,
    object: O,
    _marker: std::marker::PhantomData<(T, Id)>,
}

impl<T: Clone, V: LocalView<T, Id>, O: TransactionalObject<T, Id>, Id: Copy>
    LocalViewManager<T, V, O, Id>
{
    pub(crate) fn new(view: V, object: O) -> Self {
        Self {
            view,
            object,
            _marker: Default::default(),
        }
    }

    pub(crate) fn view(&self) -> &V {
        &self.view
    }

    pub(crate) fn view_mut(&mut self) -> &mut V {
        &mut self.view
    }

    pub(crate) async fn sync(&mut self) -> Result<(), TransactionalObjectError> {
        loop {
            match self.object.update(self.view.value().clone()).await {
                Ok(_) => {
                    self.view.merge(self.object.prepare_dirty()?);
                    return Ok(());
                }
                Err(TransactionalObjectError::ObjectVersionExists) => {
                    self.object.refresh().await?;
                    self.view.merge(self.object.prepare_dirty()?)
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub(crate) async fn refresh(&mut self) -> Result<(), TransactionalObjectError> {
        self.object.refresh().await?;
        self.view.merge(self.object.prepare_dirty()?);
        Ok(())
    }

    /// Tries to apply an update to the view
    pub(crate) async fn maybe_apply_update_to_view<E: Error + Send + Sync + 'static>(
        &mut self,
        mutator: impl Fn(DirtyObject<T, Id>) -> Result<DirtyObject<T, Id>, E>,
    ) -> Result<(), TransactionalObjectError> {
        loop {
            let current = self.view.value().clone();
            let updated = mutator(current)
                .map_err(|e| TransactionalObjectError::CallbackError(Box::new(e)))?;
            match self.object.update(updated).await {
                Ok(_) => {
                    self.view.merge(self.object.prepare_dirty()?);
                    return Ok(());
                }
                Err(TransactionalObjectError::ObjectVersionExists) => {
                    self.object.refresh().await?;
                    self.view.merge(self.object.prepare_dirty()?)
                }
                Err(err) => return Err(err),
            }
        }
    }
}
