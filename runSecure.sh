
if [ -z $1 ]; then
    build/handle --info --port 8443 --key secrets/key.pem --certificate secrets/certificate.pem --log-access logs/access
else
    gdb --args build/handle --trace --port 8443 --key secrets/key.pem --certificate secrets/certificate.pem --log-access logs/access
fi


