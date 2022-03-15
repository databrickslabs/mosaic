from livereload import Server, shell

if __name__ == '__main__':
    server = Server()
    server.watch('*.rst', shell('make -C .. html'), delay=1)
    server.watch('*.md', shell('make -C .. html'), delay=1)
    server.watch('*.py', shell('make -C .. html'), delay=1)
    server.watch('api/*', shell('make -C .. html'), delay=1)
    server.watch('images/*', shell('make -C .. html'), delay=1)
    server.watch('usage/*', shell('make -C .. html'), delay=1)
    server.watch('_static/*', shell('make -C .. html'), delay=1)
    server.watch('_templates/*', shell('make -C .. html'), delay=1)
    server.serve(root='../_build/html')