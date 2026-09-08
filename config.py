import os, cherrypy
from dotenv import load_dotenv
load_dotenv()

# ----------------------------- [ Root ] -----------------------------
WEB_ROOT = os.path.abspath(os.path.dirname(__file__))
conf = {'/':
        {'tools.staticdir.on': True,
         'tools.staticdir.dir': WEB_ROOT,
         'tools.staticdir.index': 'index.html',
         'tools.sessions.on': True,
         'tools.sessions.timeout': 30,
         'tools.force_tls.on': True
         }}



serverno = os.getenv("SERVER_NO")


