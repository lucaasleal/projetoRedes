#!/bin/bash

(cd entrega1/servidor && python3 servidor.py) &
(cd entrega1/cliente && python3 cliente.py)

#chmod +x run.sh
#./run.sh