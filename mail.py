import smtplib
import mimetypes

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from os.path import basename
from email import encoders


def extract_type(file):
    ctype, encoding = mimetypes.guess_type(file)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", 1)

    return maintype, subtype


def send_mail(
    sender,                 # [string] -> email sender
    recipient,              # [string] -> email destination
    cc,                     # [string] -> email carbon copy
    subject,                # [string] -> email subject
    message,                # [string] -> email message
    filepath=[],            # [list] -> path to attachment
    username=None,          # [string] -> email server username
    password=None           # [string] -> email server password
):
    try:
        # define and setup MIME object
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = recipient
        if cc: msg['CC'] = cc
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'html' if '<html>' in message else 'plain'))

        
        # add attachment if existent
        if filepath:
            for fileToSend in filepath:
                maintype, subtype = extract_type(fileToSend)
                if maintype == "text":
                    fp = open(fileToSend)
                    # Note: we should handle calculating the charset
                    attachment = MIMEText(fp.read(), _subtype=subtype)
                    fp.close()
                elif maintype == "image":
                    fp = open(fileToSend, "rb")
                    attachment = MIMEImage(fp.read(), _subtype=subtype)
                    fp.close()
                elif maintype == "audio":
                    fp = open(fileToSend, "rb")
                    attachment = MIMEAudio(fp.read(), _subtype=subtype)
                    fp.close()
                else:
                    fp = open(fileToSend, "rb")
                    attachment = MIMEBase(maintype, subtype)
                    attachment.set_payload(fp.read())
                    encoders.encode_base64(attachment)
                    fp.close()
                attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(basename(fileToSend))
                msg.attach(attachment)

        # login to server
        # use credentials if needed
        session = smtplib.SMTP('10.97.95.27', 2525)
        if username or password:
            session.login(user=username, password=password)

        print("Email sent to {}".format(recipient))
        # send email and close session
        session.send_message(msg)
        session.close()
    except Exception as e:
        print(e)
