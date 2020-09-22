class Captcha(object):

    import sys

    from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask

    from utils.api import API
    from utils.io import IO

    def anticaptcha(self, content, url, driver):
        try:
            attempt = 1
            while 'captcha' in str(content):
                print('A page is protected by ReCaptcha. Calling AntiCaptcha...',
                      f'Attempt: {attempt}.')
                self._bypass_captcha(url, driver)
                content = driver.page_source
                attempt += 1
        except Exception as e:
            method_name = self.sys._getframe().f_code.co_name
            print(f'[Error] Method: {method_name}. Error message: {str(e)}.')
        return content

    def _bypass_captcha(self, url, driver):
        api, io = self.API(), self.IO()
        api_key = api.key(service='anticaptcha', category='key')
        site_key = self._get_sitekey(driver)
        print('site_key:', site_key)
        try:
            driver.get(url)
            client = self.AnticaptchaClient(api_key)
            task = self.NoCaptchaTaskProxylessTask(url, site_key)
            job = client.createTask(task)
            print('Waiting for AntiCaptcha solution for the page.')
            job.join()
            # Get response
            response = job.get_solution_response()
            print('Received solution for the page.')
            # Submit
            self._form_submit(driver, response)
            # Inform
            print('Captcha for the page has been bypassed.')
            io.pause(4)
        except Exception as e:
            method_name = self.sys._getframe().f_code.co_name
            print(f'[Error] Method: {method_name}. Error message: {str(e)}.')

    def _get_sitekey(self, driver):
        return driver.find_element_by_class_name("g-recaptcha").get_attribute("data-sitekey")

    def _form_submit(self, driver, response):
        io = self.IO()
        driver.execute_script(f"document.getElementById('g-recaptcha-response').innerHTML='{response}';")
        driver.find_element_by_id("recaptcha-submit").submit()
        io.pause(2)
