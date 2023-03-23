import re

email=input("Enter your email id: ")
first_name=""
last_name=""
service=""
if email.count(".")==2:
    first_name+=email[0:email.index('.')]
    first_name = re.sub('[^a-zA-Z\s]+', '',first_name)
    print(email)
    last_name+=email[email.index('.')+1:email.index('@')]
else:
    first_name = re.sub('[^a-zA-Z\s]+', '', email.split('@')[0])
index=email.index('@')
service+=email[index+1:]
print(first_name)
print(last_name)
print(service)
