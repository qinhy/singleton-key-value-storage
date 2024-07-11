class User:
    user_id = None
    email = None
    created_at = None
    updated_at = None

class App:
    App_id = None
    parent_App_id = None
    running_cost = None
    major_name = None
    minor_name = None
    created_at = None
    updated_at = None

class LicenseApp:
    license_App_id = None
    license_id = None
    App_id = None

class License:
    license_id = None
    user_id = None
    access_token = None
    bought_at = None
    expiration_date = None
    running_time = None
    max_running_time = None
    updated_at = None

class AppUsage:
    usage_id = None
    user_id = None
    App_id = None
    license_id = None
    start_time = None
    end_time = None
    running_time_cost = None
