from fastapi import APIRouter

from routes.jobs import router as jobs_router
from routes.reports import router as reports_router
from routes.scrape import router as scrape_router
from routes.system import router as system_router

router = APIRouter()
router.include_router(scrape_router)
router.include_router(jobs_router)
router.include_router(reports_router)
router.include_router(system_router)
