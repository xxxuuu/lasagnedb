use crate::meta::iterator::ManifestIterator;
use crate::meta::manifest::{Manifest, ManifestItem};
use crate::record::{RecordBuilder, RecordItem};
use std::sync::Arc;

#[test]
fn test_manifest() {
    let path = tempfile::tempdir().unwrap();
    let path = path.path();

    let items = vec![
        ManifestItem::Init(0),
        ManifestItem::NewSst(0, 1),
        ManifestItem::FreezeAndCreateWal(0, 1),
    ];
    {
        let mut m = Manifest::open(path.join("MANIFEST")).unwrap();
        for _ in 0..2 {
            let mut rbuilder: RecordBuilder<ManifestItem> = RecordBuilder::new();
            for item in &items {
                rbuilder.add(*item)
            }
            m.add(&rbuilder.build());
        }
    }

    let m = Arc::new(Manifest::open(path.join("MANIFEST")).unwrap());
    let mut manifest_iter = ManifestIterator::create_and_seek_to_first(m).unwrap();
    let mut _items = items.clone();
    _items.extend(items);
    let iter = _items.iter();
    for item in iter {
        assert!(manifest_iter.is_valid());
        assert_eq!(manifest_iter.record_item().encode(), item.encode());
        manifest_iter.next().unwrap();
    }
}
