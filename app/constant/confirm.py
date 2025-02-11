from typing import Optional

from loguru import logger


def confirms_execution(yes: Optional[bool] = False):
    if yes:
        return
    
    user_input = input('Are you sure to continue? ([y]/n): ')
    if not user_input.lower() == 'y' and not user_input == '':
        logger.warning("You cancelled the operation")
        exit()
    logger.debug("You confirmed the operation")



if __name__ == '__main__':
    confirms_execution()
    print('should see this line if confirmed')