import logging

# from .config_handler import ConfigHandler

def get_logger(name):

	# config = ConfigHandler()
	# logging.basicConfig(level=config.logging_mode)

	logging.basicConfig(level=logging.ERROR)
	
	logger = logging.getLogger(name)
	#command_handler = logging.StreamHandler()
	#command_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
	#command_handler.setFormatter(command_format)
	#logger.addHandler(command_handler)
	return logger
