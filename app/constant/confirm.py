def confirms_execution():
    user_input = input('Are you sure to continue? ([y]/n): ')
    if not user_input.lower() == 'y' and not user_input == '':
        exit()


if __name__ == '__main__':
    confirms_execution()
    print('should see this line if confirmed')