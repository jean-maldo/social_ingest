source secrets.txt
export $(cut -d= -f1 secrets.txt)