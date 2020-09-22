class AntiCaptchaDemo(object):

    from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask

    from bin.scraper.operations.engine import Engine
    from bin.scraper.operations.captcha import Captcha

    from utils.api import API
    from utils.io import IO

    def _get_sitekey(self, driver):
        return driver.find_element_by_class_name("g-recaptcha").get_attribute("data-sitekey")

    def download(self, page_url):
        engine, io, captcha, api = self.Engine(), self.IO(), self.Captcha(), self.API()
        driver = engine.initiate_engine()
        io.pause(2)
        driver.get(page_url)
        io.pause()
        content = driver.page_source
        # Fix captcha
        bad_flag = 'captcha'
        if bad_flag in content:
            api_key = api.key(service='anticaptcha', category='key')
            print('\nExtracting site key...')
            site_key = self._get_sitekey(driver)
            print('Site key was extracted:', site_key)
            client = self.AnticaptchaClient(api_key)
            task = self.NoCaptchaTaskProxylessTask(page_url, site_key)
            job = client.createTask(task)
            print('\nWaiting for AntiCaptcha solution for the page...')
            job.join()
            # Get response
            response = job.get_solution_response()
            print('Received solution for the page!')
            driver.execute_script("document.getElementById('g-recaptcha-response').innerHTML='{}';".format(response))
            driver.find_element_by_id("recaptcha-demo-submit").submit()
            print('\nCompleted.')
            io.pause(60)


if __name__ == '__main__':
    anticapcha_demo = 'https://www.google.com/recaptcha/api2/demo'
    AntiCaptchaDemo().download(anticapcha_demo)
