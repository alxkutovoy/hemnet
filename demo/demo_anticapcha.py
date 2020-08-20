class AntiCaptchaDemo(object):

    from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask

    from bin.scraper.operations.engine import Engine
    from bin.scraper.operations.helper import Helper
    from bin.scraper.operations.captcha import Captcha

    def _get_sitekey(self, driver):
        return driver.find_element_by_class_name("g-recaptcha").get_attribute("data-sitekey")

    def download(self, page_url):
        engine, helper, captcha = self.Engine(), self.Helper(), self.Captcha()
        driver = engine.initiate_engine()

        helper.pause(2)
        driver.get(page_url)
        helper.pause()
        content = driver.page_source
        # Fix captcha
        bad_flag = 'captcha'
        if bad_flag in content:
            api_key = '43dd35a02a16e89adc4efa9ae3cfd99e'
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
            helper.pause(60)


if __name__ == '__main__':
    anticapcha_demo = 'https://www.google.com/recaptcha/api2/demo'
    AntiCaptchaDemo().download(anticapcha_demo)
