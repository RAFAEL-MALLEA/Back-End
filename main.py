from fastapi import FastAPI
from routers import main, metric_config_router, metrics_router, bsale_metrics_router, notifications, products, bsale, auth_router,user_router,company_router,warehouse_router,transaction_router,metric_display_router,jl_caspana_router,reports_router,integrations_router
from database.main_db import engine, Base
from models.main_db import User,Company
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando conexión a Redis para el caché...")
    try:
        r = aioredis.from_url("redis://localhost", encoding="utf8", decode_responses=True)
        await r.ping()
        FastAPICache.init(RedisBackend(r), prefix="fastapi-cache")
        print("Caché de Redis inicializado exitosamente.")
    except aioredis.exceptions.ConnectionError as e:
        print(f"ERROR: No se pudo conectar a Redis. El caché estará deshabilitado. Error: {e}")
    yield
    print("Cerrando conexiones...")
    await FastAPICache.clear()

app = FastAPI(
    title="Inventaria API",
    description="API para clientes de Inventaria...",
    version="0.1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir solo transacciones en la documentación
app.include_router(transaction_router.router)

# Incluir los demás routers sin que aparezcan en la documentación
app.include_router(auth_router.router)
app.include_router(user_router.router)
app.include_router(company_router.router)
app.include_router(main.router)
app.include_router(products.router)
app.include_router(bsale.router)
app.include_router(bsale_metrics_router.router)
app.include_router(warehouse_router.router)
app.include_router(metrics_router.router)
app.include_router(metric_config_router.router)
app.include_router(metric_display_router.router)
app.include_router(notifications.router)
app.include_router(jl_caspana_router.router)
app.include_router(reports_router.router)
app.include_router(integrations_router.router)