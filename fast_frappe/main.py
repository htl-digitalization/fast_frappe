import frappe
from typing import Optional
import uvicorn

from fastapi import FastAPI, Depends, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fast_frappe.ctrl import init_frappe
from fast_frappe.replicache.replicache_push import router as push_router
from fast_frappe.replicache.replicache_pull import router as pull_router
from fast_frappe.socketio import sio


app = FastAPI()
origins = ["*"]  # Allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(push_router)
app.include_router(pull_router)


def add_init_frappe_to_request(request: Request, call_next):
    init_frappe()
    # validate user from cookie
    # request.cookies[]
    session = frappe.db.sql(f'select * from `tabSessions` where sid={request.cookies.sid} and username={request.cookies.username}')
    response = call_next(request, session)
    return response


@app.get("/")
def read_root():
    init_frappe()
    available_doctypes = frappe.get_list("DocType")
    settings = frappe.get_single("System Settings")
    return {
        "available_doctypes": available_doctypes,
        "settings": settings.as_dict(),
    }


# @app.middleware("http")
@app.get("/api")
def authenticate_user(request: Request):
    init_frappe() # to be removed
    return

# if __name__ == '__main__':
#     uvicorn.run("main:app", port=8000, reload=True)
