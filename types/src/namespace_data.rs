use crate::eds::EdsId;
use crate::nmt::Namespace;

/// Identifies [`Share`]s within a [`Namespace`] located on block's [`ExtendedDataSquare`].
///
/// [`Share`]: crate::Share
/// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct NamespaceDataId {
    eds_id: EdsId,
    namespace: Namespace,
}
