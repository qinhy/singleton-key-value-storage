
class Model4User:
    class AbstractObj(BaseModel):
        id: str
        rank: list = [0]
        create_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        update_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        status: str = ""
        metadata: dict = {}
        model_config = ConfigDict(arbitrary_types_allowed=True)    
        _controller: Controller4LLM.AbstractObjController = None

        def get_controller(self)->Controller4LLM.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.AbstractObjController(store,self)

    class User:
        user_id:int = 'auto increatment'
        email = None
        created_at = None
        updated_at = None

    class App:
        App_id:int = 'auto increatment'
        parent_App_id:int = 'auto increatment'
        running_cost = None
        major_name = None
        minor_name = None
        created_at = None
        updated_at = None

    class LicenseApp:
        license_App_id:int = 'auto increatment'
        license_id:int = 'auto increatment'
        App_id:int = 'auto increatment'

    class License:
        license_id:int = 'auto increatment'
        user_id:int = 'auto increatment'
        access_token = None
        bought_at = None
        expiration_date = None
        running_time = None
        max_running_time = None
        updated_at = None

    class AppUsage:
        usage_id:int = 'auto increatment'
        user_id:int = 'auto increatment'
        App_id:int = 'auto increatment'
        license_id:int = 'auto increatment'
        start_time = None
        end_time = None
        running_time_cost = None
