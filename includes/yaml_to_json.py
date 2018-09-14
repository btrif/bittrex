#  Created by Bogdan Trif on 10-09-2018 , 2:23 PM.
# convert yaml to json
# pip3 install pyyaml
# http://pyyaml.org/wiki/PyYAMLDocumentation
# py3 yaml2json.py < ~/code/manpow/homeland/heartland/puphpet/config.yaml
# gist https://gist.github.com/noahcoad/51934724e0896184a2340217b383af73


### This converts yamls STDIN ( console copy+paste) to STDOUT ( Console )

import yaml, json, sys

sys.stdout.write(json.dumps(yaml.load(sys.stdin), sort_keys=True, indent=4))