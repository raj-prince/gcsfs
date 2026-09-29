use google_cloud_storage::client::{Storage, StorageControl};
use pyo3::exceptions::PyIOError;
use pyo3::PyResult;
use tokio::sync::OnceCell as AsyncOnceCell;

static GRPC_CLIENT: AsyncOnceCell<Storage> = AsyncOnceCell::const_new();
static CONTROL_CLIENT: AsyncOnceCell<StorageControl> = AsyncOnceCell::const_new();

pub fn init_runtime() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(16).enable_all();
    pyo3_async_runtimes::tokio::init(builder);
}

pub async fn grpc_client() -> PyResult<&'static Storage> {
    GRPC_CLIENT
        .get_or_try_init(|| async {
            Storage::builder()
                .build()
                .await
                .map_err(|e| PyIOError::new_err(format!("failed to build GCS gRPC client: {e}")))
        })
        .await
}

pub async fn control_client() -> PyResult<&'static StorageControl> {
    CONTROL_CLIENT
        .get_or_try_init(|| async {
            StorageControl::builder()
                .build()
                .await
                .map_err(|e| PyIOError::new_err(format!("failed to build StorageControl client: {e}")))
        })
        .await
}

