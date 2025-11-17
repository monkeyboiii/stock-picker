
from loguru import logger


def confirms_execution(
    action: str | None = "",
    defaultYes: bool | None = True,
    yes: bool | None = False,
):
    if yes:
        return

    if action:
        logger.critical(action)

    if defaultYes:
        user_input = input('🚨 Are you sure to continue? ([y]/n): ')
        confirmed  = user_input.lower() == 'y' or user_input == ''
    else:
        user_input = input('🚨 Are you sure to continue? (y/[n]): ')
        confirmed  = user_input.lower() == 'y'

    if confirmed:
        logger.info("You confirmed the operation")
    else:
        logger.warning("You cancelled the operation")
        exit()


if __name__ == '__main__':
    confirms_execution("Dropping a dookie...", defaultYes=False)
    logger.success('should see this line if confirmed')
