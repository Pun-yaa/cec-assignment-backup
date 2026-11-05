import pytest

from loadtests.course import course_tools_fingerprint


@pytest.fixture
def sources(tmp_path):
    files = {
        "Cargo.toml": '[workspace]\nmembers=["demo"]\n',
        "Cargo.lock": "locked versions",
        ".dockerignore": "source allowlist",
        "deploy/CourseTools.Dockerfile": "FROM rust",
        "deploy/CourseTools.Dockerfile.dockerignore": "source allowlist",
        "notifications-service/ca.crt": "public certificate",
        "demo/Cargo.toml": "[package]",
        "demo/src/lib.rs": "fn example() {}",
        ".sqlx/query.json": "{}",
        "experiment-producer/schemas/measurement.avsc": "{}",
    }
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tmp_path


def test_private_files_and_application_changes_do_not_rebuild_instructor_tools(sources):
    before = course_tools_fingerprint(sources)
    for name in (".env", "demo/auth/private.key", "temperature_observability/data.py"):
        path = sources / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("unrelated or private contents")
    assert course_tools_fingerprint(sources) == before


@pytest.mark.parametrize(
    "name",
    [
        "Cargo.lock",
        "demo/src/lib.rs",
        ".sqlx/query.json",
        "experiment-producer/schemas/measurement.avsc",
    ],
)
def test_changed_public_inputs_invalidate_the_compiled_tools(sources, name):
    before = course_tools_fingerprint(sources)
    path = sources / name
    path.write_text(path.read_text() + "changed")
    assert course_tools_fingerprint(sources) != before
