use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

pub fn open_file<P: AsRef<Path>>(file_name: P) -> std::io::Result<File> {
    let file_name = file_name.as_ref();
    let path = Path::new(file_name);
    File::open(path)
}

pub fn hashset(data: Vec<i32>) -> HashSet<i32> {
    HashSet::from_iter(data.iter().cloned())
}
