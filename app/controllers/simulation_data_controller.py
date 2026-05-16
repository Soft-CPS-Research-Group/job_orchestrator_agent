from app.services import simulation_data_service


def index_simulation_data(job_id: str, session: str | None = "latest"):
    return simulation_data_service.index_simulation_data(job_id=job_id, session=session)


def read_simulation_data_file(job_id: str, relative_path: str, session: str | None = "latest"):
    return simulation_data_service.read_simulation_data_file(
        job_id=job_id,
        relative_path=relative_path,
        session=session,
    )
