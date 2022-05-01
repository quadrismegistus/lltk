from lltk.imports import *
from loguru import logger
LOGGER=None
HIDDEN_NOW=False



class Logger():
    def __init__(self,
            to_screen=True,
            to_file=True,
            fn=None,
            verbose=1,
            start=True,
            format="""[{time:HH:mm:ss}] {name}.<level>{function}</level>( <cyan>{message}</cyan> )""",
            fn_clear=True,
            fn_rotation="50MB",
            ):
        # set attrs
        self.format=format
        self.to_screen=to_screen
        self.id_screen=None

        self.to_file=to_file
        self.id_file=None
        self.fn_clear=fn_clear
        self.fn_rotation=fn_rotation
        self.fn=fn

        self.verbose = verbose # general verbosity!

        # clear
        logger.remove()

        # start?
        if start: self.start()

    @property
    def v(self): return self.verbose

    def __lt__(self, other_num): return self.verbose<other_num
    def __le__(self, other_num): return self.verbose<=other_num
    def __gt__(self, other_num): return self.verbose>other_num
    def __ge__(self, other_num): return self.verbose>=other_num
    def __eq__(self, other_num): return self.verbose==other_num
    def __ne__(self, other_num): return self.verbose!=other_num
    def __bool__(self): return bool(self.verbose)
    __nonzero__=__bool__

    def set_verbose(self,verbose=1): self.verbose=1

    def __enter__(self):
        self.start_screen()
    def __exit__(self,*x): 
        if not self.to_screen: self.stop_screen()
    
    def start(self):
        if self.to_screen: self.start_screen()
        if self.to_file: self.start_file()

    def stop(self):
        self.stop_file()
        self.stop_screen()


    def start_screen(self):
        if self.id_screen is None:
            if not self.verbose: self.verbose=1
            self.id_screen=logger.add(sys.stderr, colorize=True, format=self.format)
            #logger.debug(f'log added: {self.id_screen}')
    
    def stop_screen(self):
        if self.id_screen is not None:
            #logger.debug('removing sceen log')
            logger.remove(self.id_screen)
            self.id_screen = None


    def start_file(self):
        if self.id_file is None and self.fn:
            from lltk.tools.tools import ensure_dir_exists,backup_fn,rmfn
            ensure_dir_exists(self.fn)
            if os.path.exists(self.fn): backup_fn(self.fn)
            rmfn(self.fn)
            
            self.id_file=logger.add(self.fn, rotation=self.fn_rotation, colorize=False, format=self.format)
            #logger.debug(f'log added: {self.id_file}')

    def stop_file(self):
        if self.id_file is not None:
            #logger.debug('removing file log')
            logger.remove(self.id_file)
            self.id_file = None

            # if self.fn_clear and self.fn:
                #logger.debug(f'removing log file: {self.fn}')
                # rmfn(self.fn)

    def __getattr__(self,name):
        from lltk.tools.tools import getattribute
        res = getattribute(self,name)
        if res is None: res = getattribute(logger,name)
        return res

    def hidden(self,verbose=0):
        return log_hidden(verbose=verbose,log=self)
    def shown(self,verbose=1):
        return log_shown(verbose=verbose,log=self)

    @property
    def showing(self): return self.shown()
    @property
    def hiding(self): return self.hidden()
    q=hiding
    quiet=hiding
    shh=hiding

    def hide(self):
        self.to_screen=False
        self.stop_screen()

    def show(self):
        self.to_screen=True
        self.start_screen()
        

    
    # def hide(self,verbose=0):
    #     if self.verbose>1: self('hiding log')
    #     self.verbose_was=self.verbose
    #     self.verbose=verbose
    # def show(self,verbose=1):
    #     self.verbose_was=self.verbose
    #     self.verbose=verbose
    #     if self.verbose>1: self('showing log')
    
    __call__ = logger.debug


def Log(force=False,**kwargs):
    global LOGGER
    if force or LOGGER is None:
        LOGGER = Logger(**kwargs)
    return LOGGER

class log_hidden():
    def __init__(self,verbose=0,log=None):
        self.log=log if log is not None else Log()
        # self.verbose=verbose
    def __enter__(self): 
        # self.log.verbose_was=self.log.verbose
        # self.log.verbose=self.verbose
        self.log.stop_screen()
    def __exit__(self,*x):
        # self.log.verbose=self.log.verbose_was
        # self.log.verbose_was=self.verbose
        if self.log.to_screen: self.log.start_screen()

class log_shown():
    def __init__(self,verbose=1,log=None):
        self.log=log if log is not None else Log()
        # self.verbose=verbose
    def __enter__(self): 
        # self.log.verbose_was=self.log.verbose
        # self.log.verbose=self.verbose
        self.log.start_screen()
    def __exit__(self,*x):
        # self.log.verbose=self.log.verbose_was
        # self.log.verbose_was=self.verbose
        if not self.log.to_screen: self.log.stop_screen()

def hide_log(): return log_hidden()
def show_log(): return log_shown()

