class Engine:

    import psutil

    from selenium import webdriver

    def initiate_engine(self):
        if self.engine_status:
            self.shutdown_engine()
            return self.webdriver.Safari()
        else:
            return self.webdriver.Safari()

    def shutdown_engine(self):
        for process in self.psutil.process_iter(['pid', 'name']):
            if process.info['name'] == 'Safari':
                process = self.psutil.Process(process.info['pid'])
                process.terminate()

    def engine_status(self):
        for process in self.psutil.process_iter(['pid', 'name']):
            if process.info['name'] == 'Safari':
                return True
            else:
                return False
