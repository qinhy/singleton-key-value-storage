from pydantic import BaseModel
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, FileResponse
from fastapi import APIRouter, Depends, FastAPI, HTTPException, status, Request
from jose import JWTError, jwt
import requests
from datetime import datetime, timedelta, timezone
import sqlite3
import secrets
import os
from SingletonStorage.UserModel import UsersStore,Model4User

INVITE_CODE = os.environ.get('APP_INVITE_CODE', '123')
SECRET_KEY = os.environ.get('APP_SECRET_KEY', secrets.token_urlsafe(32))

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
SESSION_DURATION = ACCESS_TOKEN_EXPIRE_MINUTES * 60
UVICORN_PORT = 8000
EX_IP = requests.get('https://v4.ident.me/').text

db = UsersStore()
db.redis_backend()
router = APIRouter()
#######################################################################################


class UserModels:
    class User(Model4User.User):
        pass
    
        # username:str
        # full_name: str
        # role:str = 'user'
        # hashed_password:str # text2hash2base64Str(password),
        # email:str
        # disabled: bool=False

    class RegisterRequest(BaseModel):
        username: str
        full_name: str
        email: str
        password: str
        invite_code: str

    class EditUserRequest(BaseModel):
        # username: str
        full_name: str
        # email: str
        new_password: str
        is_remove: bool
        password: str

class AuthService:

    @staticmethod
    def create_access_token(*, data: dict, expires_delta: timedelta = None):
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + \
                timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        to_encode.update({"exp": expire})
        return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    @staticmethod
    async def get_current_user(request: Request):
        token = request.session.get("app_access_token")
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            email: str = payload.get("email")
            if email is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

        except JWTError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail="Invalid authentication credentials")

        user = db.find_user_by_email(email)
        if user.disabled:
            raise HTTPException(status_code=400, detail="Inactive user")

        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        return user


class OAuthRoutes:
    @staticmethod
    @router.get("/register", response_class=HTMLResponse)
    async def get_register_page():
        return FileResponse(os.path.join(os.path.split(__file__)[0], 'data', 'templates', "register.html"))

    @staticmethod
    @router.post("/register")
    async def register_user(request: UserModels.RegisterRequest):
        data = request.model_dump()
        if data.pop('invite_code') != INVITE_CODE:
            raise HTTPException(status_code=400, detail="invite code not valid")
        try:
            data['hashed_password'] = UserModels.User.hash_password(data.pop('password'))
            res = []
            res.append( db.add_new_user(**data) )
            return {"status": "success", "message": "User registered successfully"}
        except sqlite3.IntegrityError:
            raise HTTPException(
                status_code=400, detail="Username already exists")

    @staticmethod
    @router.post("/edit")
    async def edit_user_info(request: UserModels.EditUserRequest, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        """Edit user information."""
        # First, verify the password
        if not current_user.check_password(request.password):
            raise HTTPException(status_code=400, detail="Incorrect password")
        try:
            # Update user info
            if request.new_password == '': request.new_password = request.password
            current_user.get_controller().update(full_name=request.full_name,
                                                 email=current_user.email,
                                                 hashed_password=UserModels.User.hash_password(request.new_password),
                                                 disabled=current_user.disabled)
            return {"status": "success", "message": "User info updated successfully"}
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to update user info: {e}")

    @staticmethod
    @router.post("/remove")
    async def remove_account(request: UserModels.EditUserRequest, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        # First, verify the password
        if not current_user.check_password(request.password):
            raise HTTPException(status_code=400, detail="Incorrect password")
        try:
            current_user.get_controller().delete()
            return {"status": "success", "message": "User account removed successfully"}
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to remove account: {e}")

    @staticmethod
    @router.get("/login", response_class=HTMLResponse)
    async def get_login_page():
        return FileResponse(os.path.join(os.path.split(__file__)[0], 'data', 'templates', "login.html"))

    @staticmethod
    @router.get("/edit", response_class=HTMLResponse)
    async def get_edit_page():
        return FileResponse(os.path.join(os.path.split(__file__)[0], 'data', 'templates', "edit.html"))

    @staticmethod
    @router.post("/token")
    def get_token(form_data: OAuth2PasswordRequestForm = Depends(), request: Request = None):
        print(form_data.__dict__)
        email = form_data.username
        user = db.find_user_by_email(email)
        
        if not user or not user.check_password(form_data.password):
            raise HTTPException(
                status_code=400, detail="Incorrect username or password")

        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = AuthService.create_access_token(
            data={"email": email}, expires_delta=access_token_expires
        )
        request.session["uuid"] = user.get_id()
        request.session["app_access_token"] = access_token
        
        return {"app_access_token": access_token, "token_type": "bearer", "uuid": user.get_id()}

    @staticmethod
    @router.get("/me")
    async def read_users_me(current_user: UserModels.User = Depends(AuthService.get_current_user)):
        data = current_user.model_dump()
        data['uuid'] = current_user.get_id()
        return data

    @staticmethod
    @router.get("/session")
    def read_session(request: Request, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        return {**current_user.model_dump(), "app_access_token": request.session.get("app_access_token", "")}

    @staticmethod
    @router.get("/", response_class=HTMLResponse)
    async def read_home(current_user: UserModels.User = Depends(AuthService.get_current_user)):
        this_dir, this_filename = os.path.split(__file__)
        return FileResponse(os.path.join(this_dir, 'data', 'templates', 'edit.html'))

    @router.get("/icon/{icon_name}", response_class=HTMLResponse)
    async def read_home(request: Request, icon_name: str, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        this_dir, this_filename = os.path.split(__file__)
        return FileResponse(os.path.join(this_dir, 'data', 'icon', icon_name))

    @staticmethod
    @router.get("/logout")
    async def logout(request: Request, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        # Clear the session
        request.session.clear()
        return {"status": "logged out"}


from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
app = FastAPI(title="app")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[ '*',],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, max_age=SESSION_DURATION)

app.include_router(router, prefix="", tags=["users"])
