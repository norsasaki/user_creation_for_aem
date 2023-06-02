# user_creation_for_aem

## Overview
This script can create users and add users to groups according to csv. And it can export groups which a user belong to.

## Requirement
- Python3

## Usage
First at all, you need to prepate user list with csv format. See sample_users.csv for details. Next, update the URL and environment name in user_creation.yaml to match your environment.

Now, you are ready to create users. Try to execute the command below.
```
python3 user_creation.py --userlist config/sample_users.csv --mode import --target LOCAL
```

This is a sample result of the command
```
INFO - target environment: LOCAL, http://localhost:4502
INFO - Created user successfully: thomas_local
INFO - Added thomas_local to administrators successfully
INFO - Created user successfully: jansson_local
INFO - Added jansson_local to contributor successfully
INFO - Created user successfully: larsson_local
INFO - Added larsson_local to dam-users successfully
```


When you want to get user and group which a user belong to, try to execte the command below.
```
python3 user_creation.py --userlist config/sample_users.csv --mode export --target LOCAL
```

This is a sample result of the command
```
INFO - target environment: LOCAL, http://localhost:4502
INFO - thomas_local,administrators
INFO - jansson_local,Contributors
INFO - larsson_local,DAM Users
INFO - bengtsson_local,Authors
INFO - jacobsen_local,administrators
```

## Features
- create users and add users to groups according to csv 
- User names can be defined for each environment if the user name changes from environment to environment
- export groups which a user belong to.

## Reference

## Licence

[MIT](https://......)