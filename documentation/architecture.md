# Project Architecture

## Pipeline

## Project Components

### Clients & Resources

#### 1. BucketClient
- _validate_file_format(filename: str):

- _retry_wtih_backoff(
    operation,
    *args,
    **kwargs,
    ):

- upload_file(
    bucket_path: BucketPath,
    file_path: Path,
    file_obj: BinaryIO,
    metadata: Dict[str, str]
    ) -> bool:

- download_file(
    bucket_path: BucketPath,
    local_path: Optional[Path] = None
    ) -> Optional[bytes]:

- list_objects(
    bucket: str,
    prefix: str = "",
    max_keys: int = 1000
    ) -> Optional[List[str]]:

- delete_file(bucket_path: BucketPath) -> bool:

- file_exists(bucket_path: BucketPath) -> bool:

- create_bucket(bucket_name: str) -> bool:

- delete_bucket(bucket_name: str, force: bool = False) -> bool:

- batch_upload(
    files: List[Tuple[BucketPath, Path]],
    metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, bool]:

- batch_download(files: List[Tuple[BucketPath, Path]]) -> Dict[str, bool]: