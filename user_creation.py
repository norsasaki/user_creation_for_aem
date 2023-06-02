# -*- coding: utf-8 -*-
from optparse import OptionParser
import sys
import shutil
import os
import pathlib
import csv
import re
import time
from functools import reduce
import subprocess
import multiprocessing
import requests
import urllib.parse
import yaml
import json
import logging
import logging.handlers
from tabulate import tabulate
from dictknife import deepmerge
import pprint




# define global variables
_filename = os.path.basename(__file__)
filename = os.path.splitext(_filename)[0]
subdir = f"{filename}"
config_file = str(pathlib.Path(f"./config/{filename}.yaml"))

# define logger
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
rh = logging.handlers.RotatingFileHandler(
        f'{filename}.log', 
        encoding='utf-8',
        maxBytes=1024000,
        backupCount=2
    )
rh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(name)s - %(funcName)s - %(message)s')
rh.setFormatter(rh_formatter)
log.addHandler(rh)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(ch_formatter)
log.addHandler(ch)



# define global variables
url=""
user=""
password=""

# define option parser
parser = OptionParser()
parser.add_option("-u", "--userlist", dest="userlist")
parser.add_option("-w", "--work_dir", dest="work_dir")
parser.add_option("-m", "--mode", dest="mode", help="mode: export or import")
parser.add_option("-d", "--dryrun", action="store_true", dest="dryrun", default=True)
parser.add_option("-e", "--execute", action="store_false", dest="dryrun")
parser.add_option("-t", "--target", dest="target")

# load config
with open(config_file, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)
    (options, args) = parser.parse_args()
    if not options.userlist == None:
        config["userlist"] = options.userlist

    if not options.work_dir == None:
        config["work_dir"] = options.work_dir

    if not options.mode == None:
        config["mode"] = options.mode

    if not "dryrun" in config:
        config["dryrun"] = options.dryrun

    if not "target" == None:
        config["target"] = options.target

def text2dict(criteria):
    params = {}
    for line in criteria.replace(" ", "").split("\n"):
        if not line:
            continue

        (key, val) = line.split("=")
        params[key] = val
    return params

class UserMigration:

    domain = "http://localhost:4502"
    api_user = "admin"
    api_password = "admin"
    dryrun = True

    def __init__(self, opt):
        self.domain = opt["url"]
        self.api_user = opt["user"]
        self.api_password = opt["password"]
        self.dryrun = opt["dryrun"]

        return

    def query_builder(self, criteria):
        api_uri = "/bin/querybuilder.json"
        params = deepmerge({"p.hits": "full", "p.limit": "-1"}, criteria)

        auth = (self.api_user, self.api_password)
        try:
            r = requests.get(f"{self.domain}{api_uri}", params=params, auth=auth)

            if not r.status_code == 200:
                return {}
            return r.json()["hits"]
        except: 
            raise UserMigration('Except error was happend when requesting a query builder request')

    def query_node(self, node_path):
        if not node_path.endswith(".json"):
            node_path = f"{node_path}.10.json"
 
        auth = (self.api_user, self.api_password)
        try:
            r = requests.get(f"{self.domain}{node_path}", auth=auth)

            if not r.status_code == 200:
                return {}
            return r.json()
        except: 
            raise UserMigration('Except error was happend when requesting a query json of node')

    def get_groups_having_uuid(self, user_uuid):
        criteria = text2dict(f'''
                path=/home/groups
                type=rep:Group
                property=rep:members
                property.value={user_uuid}
            ''')
        
        return self.query_builder(criteria)

    def get_group_by_uuid(self, uuid):
        criteria = text2dict(f'''
                path=/home/groups
                type=rep:Group
                property=jcr:uuid
                property.value={uuid}
            ''')
        
        return self.query_builder(criteria)

    def get_group_by_name(self, name):
        criteria = text2dict(f'''
                path=/home/groups
                type=rep:Group
                property=rep:authorizableId
                property.value={name}
            ''')
        
        return self.query_builder(criteria)

    def get_group_by_name2(self, name):
        criteria = text2dict(f'''
                path=/home/groups
                type=rep:Group
                property=rep:authorizableId
                property.value={name}
            ''')
        
        ret = self.query_builder(criteria)

        return self.query_node(ret[0]["jcr:path"])

    def group_exists(self, name):       
        return len(self.get_group_by_name(name))

    def get_user_by_uuid(self, uuid):
        criteria = text2dict(f'''
                path=/home/users
                type=rep:User
                property=jcr:uuid
                property.value={uuid}
            ''')
        
        return self.query_builder(criteria)

    def get_user_by_name(self, name):
        criteria = text2dict(f'''
                path=/home/users
                type=rep:User
                property=rep:authorizableId
                property.value={name}
            ''')
        
        return self.query_builder(criteria)

    def user_exists(self, name):

        return len(self.get_user_by_name(name))

    def add_user_to_group(self, user_name, group_name):

        auth = (self.api_user, self.api_password)
        try:
            group = self.get_group_by_name(group_name)[0]
            api_uri = group['jcr:path'] + ".rw.html"
            payload = {"addMembers": user_name}

            r = requests.post(f"{self.domain}{api_uri}", data=payload, auth=auth)
            if r.status_code == 200:
                log.info(f"Added {user_name} to {group_name} successfully")
            else:
                log.warning(f"Failed to add {user_name} to {group_name}")

            return r.status_code
        except: 
            raise UserMigration('Except error was happend when adding a user to a group')

    def add_user_to_groups(self, user_name, group_name_list):
        try:
            all_groups_exist = True
            for group_name in group_name_list:
                if not self.group_exists(group_name):
                    log.warning(f"{group_name} doesn't exist")
                    all_groups_exist = False

            if all_groups_exist:
                for group_name in group_name_list:
                    self.add_user_to_group(user_name, group_name)
            else:
                log.warning("Some groups were not found. So, skipped to add this user to any groups.")

        except:
            raise UserMigration('Except error was happend when adding a user to a group')               

    def create_user(self, user_info):
        api_uri = "/libs/granite/security/post/authorizables"

        payload = deepmerge({
                "createUser": "",
                "authorizableId": "",
                "rep:password": "",
                "profile/email": "",
                "profile/familyName": "",
                "profile/givenName": ""
            }, user_info)

        auth = (self.api_user, self.api_password)
        try:
            if not self.user_exists(user_info["authorizableId"]):
                r = requests.post(f"{self.domain}{api_uri}", data=payload, auth=auth)

                if r.status_code == 201:
                    log.info("Created user successfully: " + user_info["authorizableId"])
                else:
                    log.warning("Failed to create user: " + user_info["authorizableId"])

                return r.status_code
            else:
                log.warning(user_info["authorizableId"] + " has already existed. skip to create this user.")
                return 401
        except: 
            raise UserMigration('Except error was happend when creating a new user')




def initalize():
    subdir = config["work_dir"]
    env_directory = str(pathlib.Path(f"./{subdir}/" + config["target"]))   

    if not os.path.exists(subdir): 
        os.mkdir(subdir)
    if os.path.exists(env_directory): 
        shutil.rmtree(env_directory)
    if not os.path.exists(env_directory): 
        os.mkdir(env_directory)


    return

def read_userlist(path):
    try:
        with open(path, 'r',encoding="utf-8_sig") as f:
            reader = csv.reader(f, dialect='excel')
            header = []
            userlist = []
            for row in reader:
                if len(header) == 0:
                    header = row
                else:
                    userlist.append({key: val for key, val in zip(header, row)})
    except:
        log.critical(f'Unexpected error happen while reading {path}')
        sys.exit(1)

    return userlist

def ok(evaluation, description):
    if evaluation:
        log.info(f"[ok] - {description}")
    else:
        log.info(f"[not ok] - {description}")

    return

def on_import(config):
    target = config["target"]

    # generage UserMigration object
    for env in config["environment"]:
        if env["name"] == target:
            log.info(f"target environment: {target}, " + env["url"])
            opt = {
                "url": env["url"],
                "user": env["user"],
                "password": env["password"],
                "dryrun": config["dryrun"]
            }

    um = UserMigration(opt)

    # read userlist and get username
    userlist = read_userlist(config["userlist"])

    # create user and add user to group
    for user in userlist:
        username = user[target]
        user_info = {
                "authorizableId": username,
                "rep:password": user["password"],
                "profile/email": user["email"],
                "profile/familyName": user["familyName"],
                "profile/givenName": user["givenName"]
            }

        ret = um.create_user(user_info)
        if ret == 201:
            groups = user["groups"].split('|')
            um.add_user_to_groups(username,groups)
        else:
            groups = user["groups"].split('|')
            um.add_user_to_groups(username,groups)

    return

def on_export(config):
    target = config["target"]

    # generage UserMigration object
    for env in config["environment"]:
        if env["name"] == target:
            log.info(f"target environment: {target}, " + env["url"])
            opt = {
                "url": env["url"],
                "user": env["user"],
                "password": env["password"],
                "dryrun": config["dryrun"]
            }

    um = UserMigration(opt)

    # read userlist and get username
    userlist = read_userlist(config["userlist"])

    # export group information
    for user in userlist:
        username = user[target]
        groupname = []

        if um.user_exists(username):
            u = um.get_user_by_name(username)
            groups = um.get_groups_having_uuid(u[0]["jcr:uuid"])
            for group in groups:
                g = um.get_group_by_name2(group["rep:authorizableId"])

                if "profile" in g:
                    if "givenName" in g["profile"]:
                        groupname.append(g["profile"]["givenName"])
                    else: 
                        groupname.append(g["rep:authorizableId"])


#                print(um.get_group_by_name2(group["rep:authorizableId"]))
#                groupname.append(group["rep:authorizableId"])

        log.info(f"{username}," + "|".join(groupname)) 

#    def user_exists(self, name):

#    def user_exists(self, name):
#    def get_groups_having_uuid(self, user_uuid):
#    def get_group_by_uuid(self, uuid):



    sys.exit()

    return

def on_compare(config):
    log.debug("on compare")
    return

def main():
    initalize()

    event_hander = {"import": on_import, "export": on_export, "compare": on_compare}
    for key in event_hander.keys():
        if config["mode"] == key:
            event_hander[config["mode"]](config)

    sys.exit()
    global url, user, password

    url = "http://localhost:4502"
    user = "admin"
    password = "admin"



    if config["mode"] == "import":
        url=config["destination"]["url"]
        user=config["destination"]["user"]
        password=config["destination"]["password"]
        source_env = config["source"]["name"]
        env = config["destination"]["name"]


        # read userlist and get username
        userlist = read_userlist(config["userlist"])

        # .
        for row in userlist:
            username = row[source_env]
            new_username = row[env]

            input = str(pathlib.Path(f"./" + config["work_dir"] + f"/{source_env}/{username}.json"))
            with open(input) as f:
                group_info= json.load(f)

            # confirm all groups in json file exist
            exist_all_group = True
            for group in group_info:
                if not get_group( group["rep:authorizableId"] ):
                    log.warning( group["rep:authorizableId"] + " doesn't exist")
                    exist_all_group = False
                    break
                
            if not exist_all_group:
                log.warning( "skip creating " + new_username)
                break

            # create user
            if 0 == create_user(new_username, row["password"], row["firstname"], row["lastname"], row["email"] ):
                for group in group_info:
                    # add user to group
                    g = get_group( group["rep:authorizableId"] )                    
                    add_user_to_group(new_username, g[0]["jcr:path"])

            # query group list which user is belong to.
            uuid = get_uuid_by_username(new_username)
            group_list = get_groups_having_uuid(uuid)
            log.debug(f"username: {new_username}, uuid: {uuid}, group_list: " + json.dumps(group_list))
            if len(group_list) > 0:
                output_dir = str(pathlib.Path(f"./" + config["work_dir"] + f"/{env}"))
                output = str(pathlib.Path(f"./" + f"{output_dir}/{new_username}.json"))
                if not os.path.exists(output_dir): 
                    os.mkdir(output_dir)
                with open(output, 'w') as f:
                    json.dump(group_list, f, indent=4)







    elif config["mode"] == "export":

        url=config["source"]["url"]
        user=config["source"]["user"]
        password=config["source"]["password"]
        env = config["source"]["name"]

        # read userlist
        userlist = read_userlist(config["userlist"])

        # query group list which user is belong to.
        for row in userlist:
            username = row[env]
            uuid = get_uuid_by_username(username)
            group_list = get_groups_having_uuid(uuid)
            log.debug(f"username: {username}, uuid: {uuid}, group_list: " + json.dumps(group_list))
            if len(group_list) > 0:
                output_dir = str(pathlib.Path(f"./" + config["work_dir"] + f"/{env}"))
                output = str(pathlib.Path(f"./" + f"{output_dir}/{username}.json"))
                if not os.path.exists(output_dir): 
                    os.mkdir(output_dir)

                with open(output, 'w') as f:
                    json.dump(group_list, f, indent=4)

    else:
        source_env = config["source"]["name"]
        destination_env = config["destination"]["name"]
        source_dir = str(pathlib.Path(f"./" + config["work_dir"] + f"/{source_env}"))
        destination_dir = str(pathlib.Path(f"./" + config["work_dir"] + f"/{destination_env}"))

        # read userlist and get username
        userlist = read_userlist(config["userlist"])

        # .
        try:
            for row in userlist:
                source_username = row[source_env]
                destination_username = row[destination_env]

                source_json = str(pathlib.Path(f"./{source_dir}/{source_username}.json"))
                destination_json = str(pathlib.Path(f"./{destination_dir}/{destination_username}.json"))

                with open(source_json) as f:
                    _source_group= json.load(f)
                    source_group = list(map(lambda x: x["rep:principalName"], _source_group))
                    source_group.sort()
                with open(destination_json) as f:
                    _destination_group= json.load(f)
                    destination_group = list(map(lambda x: x["rep:principalName"], _destination_group))
                    destination_group.sort()

                ok(source_group == destination_group, f"compare {source_username} with {destination_username}")
                if not source_group == destination_group:
                        log.warning("\n" + tabulate({source_env: source_group, destination_env: destination_group}, headers='keys'))


        except Exception as err:
            log.error(f"Unexpected {err=}, {type(err)=}")
            sys.exit(1)
            
    return 0

main()





opt = {
    "url": "http://localhost:4502",
    "user": "admin",
    "password": "admin"
}
um = UserMigration(opt)
# print(um.query_builder(text2dict(
#     f'''
#         path=/home/groupsasdf
#         type=rep:Group
#         property=rep:principalName
#         property.value={groupname}
#     '''
# ))) 

user_info = {
        "authorizableId": "testuser",
        "rep:password": "testuser",
        "profile/email": "aa",
        "profile/familyName": "bb",
        "profile/givenName": "cc"
    }

func = ok
print(func(True, "desc"))

ret = um.create_user(user_info)
if ret == 201:
    print(um.add_user_to_groups("testuser",["dam-users", "analytics-administrators", "af-template-script-writers"]))



sys.exit(0)



def read_userlist(path):
    try:
        with open(path, 'r',encoding="utf-8_sig") as f:
            reader = csv.reader(f, dialect='excel')
            header = []
            userlist = []
            for row in reader:
                if len(header) == 0:
                    header = row
                else:
                    userlist.append({key: val for key, val in zip(header, row)})
    except:
        log.critical(f'Unexpected error happen while reading {path}')
        sys.exit(1)

    return userlist

def http_request(command):
    try:
        log.debug(f"curl commnad: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if not result.returncode == 0:
            log.error(result.stderr)
            raise ValueError("os command failed")

        res = json.loads(result.stdout)

    except Exception as err:
        log.error(f"Unexpected {err=}, {type(err)=}")
        sys.exit(1)

    return res

def query_builder(criteria):

    
    params = {"path": "/home/groups", "type": "rep:Group"}
    auth = (user, password)
    r = requests.get(f"{url}/bin/querybuilder.json", params=params, auth=(user, password))

    print(r)
    sys.exit(0)

    command = f"curl -k -sS -u {user}:{password} {url}/bin/querybuilder.json"
    for c in criteria:
        command += f" --data-urlencode \"{c}\""

    try:
        res = http_request(command)
        if not res["success"] == True:
            log.error("http_request failed")
            log.info(f"res: {res}")
            raise ValueError("os command failed")

    except Exception as err:
        log.error(f"Unexpected {err=}, {type(err)=}")
        sys.exit(1)
        
    return res["hits"]


def text2list(criteria):
    return criteria.replace(" ", "").replace('"', '\\"').split("\n")

def get_groups_having_uuid(user_uuid):
    criteria = f'''
        path=/home/groups
        type=rep:Group
        property=rep:members
        property.value={user_uuid}
        p.hits=full
        p.limit=-1
    '''.replace(" ", "").replace('"', '\\"').split("\n")

    criteria = list(filter(None, criteria))

    try:
        res = query_builder(criteria)

    except Exception as err:
        log.error(f"Unexpected {err=}, {type(err)=}")
        sys.exit(1)

    return res

def get_group(groupname):
    criteria = f'''
        path=/home/groups
        type=rep:Group
        property=rep:principalName
        property.value={groupname}
        p.hits=full
        p.limit=-1
    '''.replace(" ", "").replace('"', '\\"').split("\n")
    criteria = list(filter(None, criteria))

    try:
        res = query_builder(criteria)

    except Exception as err:
        log.error(f"Unexpected {err=}, {type(err)=}")
        res = None
        sys.exit(1)

    return res

def get_uuid_by_username(username):
    criteria = f'''
        path=/home/users
        type=rep:User
        property=rep:principalName
        property.value={username}
        p.hits=full
    '''.replace(" ", "").replace('"', '\\"').split("\n")
    criteria = list(filter(None, criteria))

    try:
        res = query_builder(criteria)
        log.debug(f"username: {username}, uuid: " + res[0]["jcr:uuid"])

    except IndexError as err:
        log.error("jcr:uuid property don't be found in the response")
        log.info(f"res: {res}")
        sys.exit(1)
        
    except Exception as err:
        log.error(f"Unexpected {err=}, {type(err)=}")
        sys.exit(1)

    return res[0]["jcr:uuid"]

# curl -u admin:admin -FaddMembers=testuser1 http://localhost:4502/home/groups/t/testGroup.rw.html
def add_user_to_group(username, group_path):
    criteria = f'''
        addMembers={username}
    '''.replace(" ", "").split("\n")
    criteria = list(filter(None, criteria))
    criteria = reduce(lambda a, b: f"{a} -F \"{b}\"", criteria, "")

    command = f"curl -k -sS -u {user}:{password} {url}{group_path}.rw.html {criteria}"

    try:
        log.debug(f"curl commnad: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if not result.returncode == 0:
            log.error(result.stderr)
            raise ValueError("os command failed")

        log.info(f"add {username} to: {group_path}")

    except Exception as err:
        log.error(f"Unexpected {err=}, {type(err)=}")
        sys.exit(1)

    return

# curl -u admin:admin -FcreateUser= -FauthorizableId=testuser -Frep:password=abc123 http://localhost:4502/libs/granite/security/post/authorizables
# curl -u admin:admin -FcreateUser=testuser -FauthorizableId=testuser -Frep:password=abc123 \
# -Fprofile/email=test@gmail.com \
# -Fprofile/familyName=test \
# -Fprofile/givenName=user \
# http://localhost:4502/libs/granite/security/post/authorizables

# curl -u admin:admin -F "createUser=testuser" -F "authorizableId=testuser" -F "rep:password=abc123" \
# -F "profile/email=test@gmail.com" \
# -F "profile/familyName=test" \
# -F "profile/givenName=user" \
# http://localhost:4502/libs/granite/security/post/authorizables



def create_user(username, pwd, familyName, givenName, email):
    criteria = f'''
        createUser={username}
        authorizableId={username}
        rep:password={pwd}
        profile/email={email}
        profile/familyName={familyName}
        profile/givenName={givenName}
    '''.replace(" ", "").replace('"', '\\"').split("\n")
    criteria = list(filter(None, criteria))
    criteria = reduce(lambda a, b: f"{a} -F \"{b}\"", criteria, "")

    command = f"curl -k -sS -u {user}:{password} {url}/libs/granite/security/post/authorizables {criteria}"

    try:
        log.debug(f"curl commnad: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if not result.returncode == 0:
            log.error(result.stderr)
            raise ValueError("os command failed")

        log.info(f"create user: {username}")

    except Exception as err:
        log.error(f"Unexpected {err=}, {type(err)=}")
        sys.exit(1)

    return result.returncode


def initalize():
    work_dir = config["work_dir"]
    if not os.path.exists(work_dir): 
        os.mkdir(work_dir)

    return

def ok(evaluation, description):
    if evaluation:
        log.info(f"[ok] - {description}")
    else:
        log.info(f"[not ok] - {description}")

    return

def main():
    global url, user, password
    initalize()

    url = "http://localhost:4502"
    user = "admin"
    password = "admin"

    query_builder("")
    sys.exit(0)


    if config["mode"] == "import":
        url=config["destination"]["url"]
        user=config["destination"]["user"]
        password=config["destination"]["password"]
        source_env = config["source"]["name"]
        env = config["destination"]["name"]


        # read userlist and get username
        userlist = read_userlist(config["userlist"])

        # .
        for row in userlist:
            username = row[source_env]
            new_username = row[env]

            input = str(pathlib.Path(f"./" + config["work_dir"] + f"/{source_env}/{username}.json"))
            with open(input) as f:
                group_info= json.load(f)

            # confirm all groups in json file exist
            exist_all_group = True
            for group in group_info:
                if not get_group( group["rep:authorizableId"] ):
                    log.warning( group["rep:authorizableId"] + " doesn't exist")
                    exist_all_group = False
                    break
                
            if not exist_all_group:
                log.warning( "skip creating " + new_username)
                break

            # create user
            if 0 == create_user(new_username, row["password"], row["firstname"], row["lastname"], row["email"] ):
                for group in group_info:
                    # add user to group
                    g = get_group( group["rep:authorizableId"] )                    
                    add_user_to_group(new_username, g[0]["jcr:path"])

            # query group list which user is belong to.
            uuid = get_uuid_by_username(new_username)
            group_list = get_groups_having_uuid(uuid)
            log.debug(f"username: {new_username}, uuid: {uuid}, group_list: " + json.dumps(group_list))
            if len(group_list) > 0:
                output_dir = str(pathlib.Path(f"./" + config["work_dir"] + f"/{env}"))
                output = str(pathlib.Path(f"./" + f"{output_dir}/{new_username}.json"))
                if not os.path.exists(output_dir): 
                    os.mkdir(output_dir)
                with open(output, 'w') as f:
                    json.dump(group_list, f, indent=4)







    elif config["mode"] == "export":

        url=config["source"]["url"]
        user=config["source"]["user"]
        password=config["source"]["password"]
        env = config["source"]["name"]

        # read userlist
        userlist = read_userlist(config["userlist"])

        # query group list which user is belong to.
        for row in userlist:
            username = row[env]
            uuid = get_uuid_by_username(username)
            group_list = get_groups_having_uuid(uuid)
            log.debug(f"username: {username}, uuid: {uuid}, group_list: " + json.dumps(group_list))
            if len(group_list) > 0:
                output_dir = str(pathlib.Path(f"./" + config["work_dir"] + f"/{env}"))
                output = str(pathlib.Path(f"./" + f"{output_dir}/{username}.json"))
                if not os.path.exists(output_dir): 
                    os.mkdir(output_dir)

                with open(output, 'w') as f:
                    json.dump(group_list, f, indent=4)

    else:
        source_env = config["source"]["name"]
        destination_env = config["destination"]["name"]
        source_dir = str(pathlib.Path(f"./" + config["work_dir"] + f"/{source_env}"))
        destination_dir = str(pathlib.Path(f"./" + config["work_dir"] + f"/{destination_env}"))

        # read userlist and get username
        userlist = read_userlist(config["userlist"])

        # .
        try:
            for row in userlist:
                source_username = row[source_env]
                destination_username = row[destination_env]

                source_json = str(pathlib.Path(f"./{source_dir}/{source_username}.json"))
                destination_json = str(pathlib.Path(f"./{destination_dir}/{destination_username}.json"))

                with open(source_json) as f:
                    _source_group= json.load(f)
                    source_group = list(map(lambda x: x["rep:principalName"], _source_group))
                    source_group.sort()
                with open(destination_json) as f:
                    _destination_group= json.load(f)
                    destination_group = list(map(lambda x: x["rep:principalName"], _destination_group))
                    destination_group.sort()

                ok(source_group == destination_group, f"compare {source_username} with {destination_username}")
                if not source_group == destination_group:
                        log.warning("\n" + tabulate({source_env: source_group, destination_env: destination_group}, headers='keys'))


        except Exception as err:
            log.error(f"Unexpected {err=}, {type(err)=}")
            sys.exit(1)
            
    return 0

main()



sys.exit()
# define global variables
_filename = os.path.basename(__file__)
filename = os.path.splitext(_filename)[0]
subdir = f"{filename}"
config_file = str(pathlib.Path(f"./{subdir}/{filename}.yaml"))
data_file = str(pathlib.Path(f"./{subdir}/uri_list.txt"))
work_dir = str(pathlib.Path(f"./{subdir}/tmp"))

# define logger
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
rh = logging.handlers.RotatingFileHandler(
        f'{filename}.log', 
        encoding='utf-8',
        maxBytes=1024000,
        backupCount=2
    )
log.addHandler(rh)


def construct_curl(config, path):

    # initialize config
    default = {
        "cmd": f"curl -sS -k ",
        "proto": "https",
        "domain": "www.google.com",
        "user-agent": "Mozilla/5.0 from test_request",
        "header": [],
        "write_out": ["http_code", "time_total"],
        "cookie": []
    }
    default.update(config)

    options = []
    options.append(["--output-dir", work_dir])
    options.append(["--output", os.getpid()])

    # make cookie
    cookie = reduce(lambda a, b: a + f"{b[0]}={b[1]};", default['cookie'], "")
    options.append(["--cookie", cookie])

    # user-agent
    options.append(["--user-agent", default['user-agent']])

    # make write_out
    write_out_options = [
         "content_type", "errormsg", "exitcode", "filename_effective", "ftp_entry_path", "http_code", "http_connect", "http_version", "local_ip", "local_port", "method", "num_connects", "num_headers", "num_redirects", "onerror", "proxy_ssl_verify_result", "redirect_url", "referer", "remote_ip", "remote_port", "response_code", "scheme", "size_download", "size_header", "size_request", "size_upload", "speed_download", "speed_upload", "ssl_verify_result", "stderr", "stdout", "time_appconnect", "time_connect", "time_namelookup", "time_pretransfer", "time_redirect", "time_starttransfer", "time_total", "url", "url_effective", "urlnum"
    ]
    write_out = "\\n" + reduce(lambda a, b: a + b + "# %{" + b + "}\\n", filter(lambda x: x in write_out_options, default["result"]), "")
    options.append(["--write-out", write_out])

    # construct request header
    for header in default['header']:
        options.append(["--header", f"{header[0]}: {header[1]}"])

    # construct uri
    if re.match("\S+://", path):
        uri = path
    else:
        path = urllib.parse.quote(path)
        uri = f"{default['proto']}://{default['domain']}{path}"
    options.append(["--dump-header -", uri])

    # make command
    return  reduce(lambda a, b: f'{a} {b[0]} "{b[1]}"', options, default['cmd'])


def curl(command):
    log.debug(command)
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if not result.returncode == 0:
        log.error(result.stderr)
        
    res_header = {}
    res_command = {}
    html = None
    for line in result.stdout.split("\n"):
        match = re.findall(r'^(HTTP\S+).+(\d\d\d)\s*$', line)
        if len(match) != 0:
            continue

        match = re.findall(r'^(\S+): (.+)$', line)
        if len(match) != 0:
            for array in match:
                res_header[array[0]] = array[1]
            continue

        match = re.findall(r'^(\S+)# (.+)$', line)
        if len(match) != 0:
            for array in match:
                res_command[array[0]] = array[1]
            continue

    if "content-type" in res_header.keys() and re.match("text/html", res_header["content-type"]):
        with open(str(pathlib.Path(f"./{subdir}/tmp/{os.getpid()}")), "r", encoding="utf-8") as f:
            html = f.read()

    return {
        "res_header": res_header,
        "res_command": res_command,
        "html": html
    }


# define worker
def worker(uri, config, result_list):

    command = construct_curl(config, uri)
    result = curl(command)

    log.debug(f"PID: {os.getpid()}")
    log.debug("res_header\n" + yaml.dump(result["res_header"]))
    log.debug("res_command\n" + yaml.dump(result["res_command"]))

    output = []
    for key in config["result"]:
        if key in result["res_header"]:
            output.append(result["res_header"][key])
            continue

        if key in result["res_command"]:
            output.append(result["res_command"][key])
            continue

        match = re.findall(r'^m(.)(.+)\1$', key)
        if len(match) != 0 and result["html"] is not None:
            regexp = match[0][1]
            match = re.findall(regexp, result["html"], re.IGNORECASE)
            if match:
                output.append(match[0])
            else:
                output.append("")
            continue

        # append blank if key doesn't match all condition
        output.append("N/A")

    
    time.sleep(config["config"]["wait"])
    return result_list.append(output)


if __name__ == "__main__":
    if not os.path.exists(subdir): 
        os.mkdir(subdir)
    if os.path.exists(work_dir): 
        shutil.rmtree(work_dir)
    if not os.path.exists(work_dir): 
        os.mkdir(work_dir)

    with open(config_file, "r", encoding="utf-8") as file:
        config = {
            "config": {"logLevel": 1, "process": 8, "wait": 0.5}
        }
        config = deepmerge(config, yaml.safe_load(file))

    with open(data_file, "r", encoding="utf-8") as f:
        uris = f.read().split("\n")

        print(uris.count("#__END__"))
        if "#__END__" in uris:
            position = uris.index("#__END__")
            uris = uris[0:position]

        uris = list(filter(lambda a: not re.match("^$|^#", a), uris))

    while len(uris) != 0:
        with multiprocessing.Manager() as manager:
            # list which is pushed to worker result
            result_list = manager.list()

            # process list
            process = []

            for i in range(config["config"]["process"]):
                if len(uris) == 0:
                    break

                process.append(multiprocessing.Process(target=worker, args=(uris.pop(0), config, result_list)))
                process[i].start()

            for p in process:
                p.join()

            print(tabulate(result_list, headers=config["result"]))


