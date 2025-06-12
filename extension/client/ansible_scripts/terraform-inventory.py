#!/usr/bin/env python

# ./terraform-inventory.py (to test)

import json
import subprocess

def get_terraform_output():
    # Run terraform output and get the public IPs as JSON
    output = subprocess.check_output(["terraform", "output", "-json", "public_ips"])
    return json.loads(output)

def generate_inventory(ips):
    inventory = {
        "clients": {
            "hosts": ips
        }
    }
    return inventory

if __name__ == "__main__":
    ips = get_terraform_output()
    inventory = generate_inventory(ips)

    print(json.dumps(inventory, indent=4))
