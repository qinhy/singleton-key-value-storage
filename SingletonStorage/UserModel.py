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
