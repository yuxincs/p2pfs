from aioconsole import ainput

__all__ = ['Cmd']


class Cmd:
    INTRO = 'Welcome'
    PROMPT = 'example > '

    def __init__(self):
        self._methods = tuple(method[3:] for method in dir(self) if method.startswith('do_'))

    async def cmdloop(self):
        print(self.INTRO)
        while True:
            raw_str = await ainput(prompt=self.PROMPT)

            command_str = raw_str.rstrip('\r\n')
            if command_str == '':
                continue
            command, *arg = command_str.split(' ', 1)
            arg = arg[0] if len(arg) == 1 else ''
            command = 'help' if command == '?' else command
            if command not in self._methods:
                print('{} is not a valid command, type \'help\' or \'?\' to see the available commands.'.format(command))
            else:
                to_stop = await self.__getattribute__('do_{}'.format(command))(arg)
                if to_stop:
                    break

    async def do_help(self, arg):
        print('Available command list:\n{}'
              .format(''.join(tuple('  -  {}\n'.format(method) for method in self._methods))))
