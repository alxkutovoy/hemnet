class Captcha(object):

    import sys

    from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask

    from bin.helpers.helper import Helper

    def anticaptcha(self, content, url, driver):
        try:
            attempt = 1
            while 'captcha' in str(content):
                print('A page is protected by ReCaptcha. Calling AntiCaptcha...',
                      'Attempt:', str(attempt) + '.')
                self._bypass_captcha(url, driver)
                content = driver.page_source
                attempt += 1
        except Exception as e:
            method_name = self.sys._getframe().f_code.co_name
            print('[Error] Method:', method_name + '.', 'Error message: ' + str(e))
        return content

    def _bypass_captcha(self, url, driver):
        helper = self.Helper()
        api_key = '43dd35a02a16e89adc4efa9ae3cfd99e'
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
            helper.pause(4)
        except Exception as e:
            method_name = self.sys._getframe().f_code.co_name
            print('[Error] Method:', method_name + '.', 'Error message:', str(e) + '.')

    def _get_sitekey(self, driver):
        return driver.find_element_by_class_name("g-recaptcha").get_attribute("data-sitekey")

    def _form_submit(self, driver, response):
        helper = self.Helper()
        driver.execute_script("document.getElementById('g-recaptcha-response').innerHTML='{}';".format(response))
        driver.find_element_by_id("recaptcha-submit").submit()
        helper.pause(2)
