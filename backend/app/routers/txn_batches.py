from fastapi import APIRouter

from app.db import connect
from app.repositories import txn_batches as txn_batches_repo

router = APIRouter(tags=["txn_batches"])


@router.get("/txn_batches")
def list_txn_batches(run_id: int | None = None):
    conn = connect()
    try:
        return txn_batches_repo.list_batches(conn, run_id=run_id)
    finally:
        conn.close()
