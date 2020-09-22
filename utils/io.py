class Utils(object):

    import os

    from os import path

    MAIN_DIRECTORY = os.path.dirname(__file__).replace("/utils", "")

    def get_full_path(self, *path):
        return self.path.join(self.MAIN_DIRECTORY, *path)
