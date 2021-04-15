import requests
import sys
import time

YARN_API_BASE_URL = 'http://es-estimation-n3.internal.ads.dailyhunt.in:8088'

def is_app_running_or_pending(app_name: str):
    resp = requests.get(YARN_API_BASE_URL+'/ws/v1/cluster/apps',
                        params={'states': 'accepted,running'},
                        timeout=20
                        ).json()
    running_or_pending_app_names = [app.get('name') for app in resp['apps']['app']]
    return app_name in running_or_pending_app_names

if __name__ == '__main__':
    app_name = sys.argv[1]
    try:
        is_running = is_app_running_or_pending(app_name)
    except:
        # One more retry after waiting for some time. If that fails, raise error
        time.sleep(5)
        is_running = is_app_running_or_pending(app_name)
    if not is_app_running_or_pending(app_name):
        sys.exit(1)
    # Otherwise end with 0 error
