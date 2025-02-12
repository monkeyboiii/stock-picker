from typing import Optional

from loguru import logger


def confirms_execution(
    action: Optional[str] = "",
    yes: Optional[bool] = False,
    defaultY: Optional[bool] = True,
):
    if yes:
        return
    
    if action:
        logger.critical(action)
    
    if defaultY:
        user_input = input(f'🚨 Are you sure to continue? ([y]/n): ')
        if not user_input.lower() == 'y' and not user_input == '':
            logger.warning("You cancelled the operation")
            exit()
        logger.debug("You confirmed the operation")

    else:
        user_input = input(f'🚨 Are you sure to continue? (y/[n]): ')
        if not user_input.lower() == 'y':
            logger.warning("You cancelled the operation")
            exit()
        logger.debug("You confirmed the operation")



if __name__ == '__main__':
    confirms_execution("Dropping a dookie...", defaultY=False)
    logger.success('should see this line if confirmed')