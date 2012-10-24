from validate_configs import validate_clusterconfig
import glob
import os
import sys

if __name__ == "__main__":
        sys.path.append('..')
    from locate_pdaq import find_pdaq_config
    config_path = find_pdaq_config()

    print "Validating all cluster configuration files"
    print "Will only print a status when a corrupt file is found"
    print "-" * 60
    print ""

    xsd_path = os.path.join(config_path, "xsd")

    invalid_found = False
    clustercfg_configs = glob.glob(os.path.join(config_path, '*.cfg'))
    for clustercfg_config in clustercfg_configs:
        valid, reason = validate_clusterconfig(clustercfg_config)

        if not valid:
            print "File is not valid! (%s)" % clustercfg_config
            print "-" * 60
            print ""
            print reason
            invalid_found = True

    if not invalid_found:
        print "No invalid cluster configuration files found"
