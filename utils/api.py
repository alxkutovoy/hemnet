class API(object):

    import json

    from utils.var import File

    def key(self, service, category):
        try:
            with open(self.File.API_KEYS) as json_file:
                api_keys = self.json.load(json_file)
                return api_keys[service][category]
        except Exception as e:
            print('There is no such API key in your list of secrets.')
