import pytest
from testcontainers.minio import MinioContainer


@pytest.fixture(scope="session", autouse=True)
def minio_url(request):
    minio = MinioContainer(
        image="minio/minio:RELEASE.2024-09-22T00-33-43Z",
        access_key="admin",
        secret_key="password",
    )
    minio.with_env("MINIO_CONSOLE_ADDRESS", ":9001")
    minio.with_exposed_ports(9000, 9001)
    minio.start()
    host_ip = minio.get_container_host_ip()
    exposed_port = minio.get_exposed_port(minio.port)

    def remove_container():
        # minio.stop()
        pass
    request.addfinalizer(remove_container)
    minio.get_client().make_bucket("warehouse")

    return f"http://{host_ip}:{exposed_port}"