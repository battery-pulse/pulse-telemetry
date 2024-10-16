# import os
# import stat
# import subprocess

# import pytest

# current_file_path = os.path.abspath(__file__)
# current_dir = os.path.dirname(current_file_path)
# scripts_dir = os.path.join(current_dir, "scripts")
# manifest_dir = os.path.join(current_dir, "manifests")


# @pytest.fixture(scope="session")
# def kind_cluster():
#     # Setup a kind cluster
#     subprocess.run(["kind", "create", "cluster"], check=True)
#     subprocess.run(["kubectl", "config", "set-context", "kind-kind"], check=True)
#     yield
#     # Teardown the kind cluster
#     subprocess.run(["kind", "delete", "cluster"], check=True)


# @pytest.fixture(scope="session")
# def kubernetes_services(kind_cluster):
#     # Setup minio and hive services
#     setup_script_path = os.path.join(scripts_dir, "setup.sh")
#     os.chmod(setup_script_path, os.stat(setup_script_path).st_mode | stat.S_IEXEC)
#     subprocess.run([setup_script_path], cwd=manifest_dir, check=True)

#     # Port-forward minio and hive services to local host
#     minio_forward = subprocess.Popen(
#         ["kubectl", "port-forward", "svc/minio", "9000:9000"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
#     )
#     hive_forward = subprocess.Popen(
#         ["kubectl", "port-forward", "svc/hive-metastore", "9083:9083"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
#     )

#     yield

#     # Terminate port-forwarding processes
#     minio_forward.terminate()
#     hive_forward.terminate()


# @pytest.fixture(scope="session")
# def seed_telemetry(kubernetes_services):
#     # Setup minio and hive services
#     setup_script_path = os.path.join(scripts_dir, "setup.sh")
#     os.chmod(setup_script_path, os.stat(setup_script_path).st_mode | stat.S_IEXEC)
#     subprocess.run([setup_script_path], cwd=manifest_dir, check=True)
#     yield


# @pytest.fixture(scope="session")
# def load_container_image(seed_telemetry):
#     pass
