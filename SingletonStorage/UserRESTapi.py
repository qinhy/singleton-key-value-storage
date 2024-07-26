
# FastAPI and Starlette Imports
from datetime import datetime, timedelta
import os
import re
import secrets
from zoneinfo import ZoneInfo
import bcrypt
from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError
import jwt
from starlette.middleware.sessions import SessionMiddleware

from pydantic import BaseModel
from UserModel import UsersStore, Model4User

######################################### connect to local key-value store
store = UsersStore()
store.mongo_backend()

######################################### fastapi
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
SESSION_DURATION = ACCESS_TOKEN_EXPIRE_MINUTES *60
SECRET_KEY = secrets.token_urlsafe(32)

INVITE_CODE = '123'
BASE_URL = ''

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=BASE_URL+"/token")
api = FastAPI()
api.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
api.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, max_age=SESSION_DURATION)
# class RESTapi:   
#     class Item(BaseModel):
#         key: str
#         value: dict = None
        
#     @api.post("/store/set/")
#     async def set_item(item: Item):
#         return store.set(item.key, item.value)

#     @api.get("/store/get/{key}")
#     async def get_item(key: str):
#         result = store.get(key)
#         if result is None:
#             raise HTTPException(status_code=404, detail="Item not found")
#         return result

#     @api.delete("/store/delete/{key}")
#     async def delete_item(key: str):
#         success = store.delete(key)
#         if not success:
#             raise HTTPException(status_code=404, detail="Item not found to delete")
#         return {"deleted": key}

#     @api.get("/store/exists/{key}")
#     async def exists_item(key: str):
#         return {"exists": store.exists(key)}

#     @api.get("/store/keys/{pattern}")
#     async def get_keys(pattern: str = '*'):
#         return store.keys(pattern)

#     @api.post("/store/loads/")
#     async def load_items(item_json: str):
#         store.loads(item_json)
#         return {"loaded": True}

#     @api.get("/store/dumps/")
#     async def dump_items():
#         return store.dumps()

#######################################################################################

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

class UserService:
    @staticmethod
    def hash_password(password: str) -> str:
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode(), salt)
        return hashed.decode()

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())

    @staticmethod
    def get_user_by_name(username: str):
        pass
        # with sqlite3.connect(DATABASE) as conn:
        #     user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
            # if user:
            #     return User.from_list(user)
    
    def get_user_by_uuid(id: str):
        pass
        # with sqlite3.connect(DATABASE) as conn:
        #     user = conn.execute("SELECT * FROM users WHERE user_uuid=?", (id,)).fetchone()
            # if user:
            #     return User.from_list(user)
            
    @staticmethod
    def format_email(email: str) -> str:
        EMAIL_REGEX = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
        if not re.match(EMAIL_REGEX, email):
            raise ValueError("Invalid email format")
        return email.lower().strip()

    @staticmethod
    def insert_user(username, full_name, email, hashed_password, user_uuid, disabled=False):
        pass
        # with sqlite3.connect(DATABASE) as conn:
        #     conn.execute("INSERT INTO users (username, full_name, email, hashed_password, user_uuid, disabled) VALUES (?, ?, ?, ?, ?, ?)", (username, full_name, email, hashed_password, user_uuid, disabled))

    @staticmethod
    def update_user_info(full_name, email, hashed_password, disabled=False):
        """Update user information."""
        pass
        # with sqlite3.connect(DATABASE) as conn:
        #     conn.execute("UPDATE users SET full_name=?, email=?, hashed_password=?, disabled=?", (full_name, email, hashed_password, disabled))

    @staticmethod
    def remove_user(uuid: str):
        pass
        # with sqlite3.connect(DATABASE) as conn:
        #     conn.execute("DELETE FROM users WHERE username=?", (username,))

class AuthService:
    class TokenData(BaseModel):        
        useruuid: str = ''

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    @staticmethod    
    def create_access_token(*, data: dict, expires_delta: timedelta = None):
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.now().replace(tzinfo=ZoneInfo("UTC")) + expires_delta
        else:
            expire = datetime.now().replace(tzinfo=ZoneInfo("UTC")) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        to_encode.update({"exp": expire})
        return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    @staticmethod
    async def get_current_user(request: Request):
        token = request.session.get("user_access_token")
        if not token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

        try:
            payload:dict = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            useruuid: str = payload.get("useruuid",None)
            if useruuid is None:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

        except JWTError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")

        user = UserService.get_user_by_uuid(useruuid)
        if user is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        return user
    
    @staticmethod
    async def get_current_user_token(token: str = Depends(oauth2_scheme)):
        try:
            payload:dict = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            useruuid: str = payload.get("useruuid",None)
            if useruuid is None:
                raise AuthService.credentials_exception
            token_data = AuthService.TokenData(useruuid=useruuid)
        except JWTError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")
        user = UserService.get_user_by_uuid(token_data.useruuid)
        if user is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        return user


    @staticmethod
    async def is_current_user_active(current_user: Model4User.User = Depends(get_current_user)):
        if current_user.disabled:
            raise HTTPException(status_code=400, detail="Inactive user")
        return current_user

    # @staticmethod
    # async def get_current_user_uuid(request: Request):
    #     user_uuid = request.session.get("uuid")
    #     if not user_uuid:
    #         raise HTTPException(status_code=401, detail="Not authenticated")
    #     return user_uuid

class OAuthRoutes:
    @staticmethod
    @api.get(BASE_URL+"/register", response_class=HTMLResponse)
    async def get_register_page():
        return FileResponse(os.path.join(os.path.split(__file__)[0], 'data', 'templates', "register.html"))

    @staticmethod
    @api.post(BASE_URL+"/register")
    async def register_user(request: RegisterRequest):
        formatted_email = UserService.format_email(request.email)
        hashed_password = UserService.hash_password(request.password)
        # user_uuid = remove_hyphen(hash2uuid(f'{formatted_email}{hashed_password}'))#str(uuid.uuid4())
        # if request.invite_code != INVITE_CODE:
        #     raise HTTPException(status_code=400, detail="invite code not valid")

        # try:
        #     res = []
        #     UserService.insert_user(request.username, request.full_name, formatted_email, hashed_password, user_uuid)
        #     res.append(UserService.unix_add_user(user_uuid))
        #     res.append(UserService.filebrowser_add_user(request.username,request.password,user_uuid))
        #     print(res)
        #     return {"status": "success", "message": "User registered successfully"}            
        #     # return RedirectResponse(BASE_URL+'/')
        # except sqlite3.IntegrityError:
        #     raise HTTPException(status_code=400, detail="Username already exists")

    @staticmethod
    @api.post(BASE_URL + "/edit")
    async def edit_user_info(request: EditUserRequest, current_user: Model4User.User = Depends(AuthService.get_current_user)):
        """Edit user information."""

        # First, verify the password
        if not UserService.verify_password(request.password, current_user.hashed_password):
            raise HTTPException(status_code=400, detail="Incorrect password")
        try:
            # Update user info
            hashed_password = UserService.hash_password(request.new_password)
            UserService.update_user_info(request.full_name, current_user.email, hashed_password, current_user.disabled)
            return {"status": "success", "message": "User info updated successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to update user info: {e}")

    @staticmethod
    @api.post(BASE_URL + "/remove")
    async def remove_account(request: EditUserRequest, current_user: Model4User.User = Depends(AuthService.get_current_user)):
        # First, verify the password
        if not UserService.verify_password(request.password, current_user.hashed_password):
            raise HTTPException(status_code=400, detail="Incorrect password")
            
        try:
            UserService.remove_user(current_user.id)
            return {"status": "success", "message": "User account removed successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to remove account: {e}")


    @staticmethod
    @api.get(BASE_URL+"/login", response_class=HTMLResponse)
    async def get_login_page():
        return FileResponse(os.path.join(os.path.split(__file__)[0], 'data', 'templates', "login.html"))
    
    @staticmethod
    @api.get(BASE_URL+"/edit", response_class=HTMLResponse)
    async def get_edit_page():
        return FileResponse(os.path.join(os.path.split(__file__)[0], 'data', 'templates', "edit.html"))

    @staticmethod
    @api.post(BASE_URL+"/token")
    def get_token(form_data: OAuth2PasswordRequestForm = Depends(), request: Request=None):
        user:Model4User.User = UserService.get_user_by_name(form_data.username)
        if not user or not UserService.verify_password(form_data.password, user.hashed_password):
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        access_token = AuthService.create_access_token(
            data={"useruuid": user.id}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        # request.session["uuid"] = user.id
        request.session["user_access_token"] = access_token        
        return {"user_access_token": access_token, "token_type": "bearer"}#, "uuid": user.id}

    @staticmethod
    @api.get(BASE_URL+"/me")
    async def read_users_me(current_user: Model4User.User = Depends(AuthService.get_current_user)):
        return {**current_user.model_dump()}
        
    @staticmethod
    @api.get(BASE_URL+"/session")
    def read_session(request: Request, current_user: Model4User.User = Depends(AuthService.get_current_user)):
        return {**current_user.model_dump(), "user_access_token": request.session.get("user_access_token","")}

    @staticmethod
    # @app.get(BASE_URL+"/{uuid}-home", response_class=HTMLResponse)
    # async def read_home(request: Request, uuid: str, user_uuid: str = Depends(AuthService.get_current_user_uuid)):
    @api.get(BASE_URL+"/", response_class=HTMLResponse)
    async def read_home(current_user: Model4User.User = Depends(AuthService.get_current_user)):
        this_dir, this_filename = os.path.split(__file__)
        return FileResponse(os.path.join(this_dir, 'data', 'templates', 'home.html'))
    
    @api.get(BASE_URL+"/icon/{icon_name}", response_class=HTMLResponse)
    async def read_home(request: Request,icon_name:str, current_user: Model4User.User = Depends(AuthService.get_current_user)):
        this_dir, this_filename = os.path.split(__file__)
        return FileResponse(os.path.join(this_dir, 'data', 'icon', icon_name))
    
    @staticmethod
    @api.get(BASE_URL+"/logout")
    async def logout(request: Request, current_user: Model4User.User = Depends(AuthService.get_current_user)):
        # Clear the session
        request.session.clear()
        return {"status": "logged out"}
    
################################################