from __future__ import division

import scrapy
import re
import urllib

from collections import OrderedDict
from scrapy.http import Request, FormRequest


DETAILS_CSS = 'table > tbody > tr > th.fctitle > a'
DISTANCE_CSS = 'p.fcdirections::text'
NEXT_PAGE_CSS = 'div.pagination * li.next a'

NAME_CSS = 'h1#org-title'
DOCTORS_CSS = 'ul.staff-list > li'
PATIENTS_XPATH = ('//h4[text()="Registered patients"]/../..'
                  '//span[@class="indicator-value"]/text()')

PERFORMANCE_URL_XPATH = '//div[@class="tabs-nav"]//a[text()="Performance"]'
PERFORMANCE_METRIC_ITEMS_CSS = 'div.metrics-wrap * div.metric-item'


class NHSGPSpider(scrapy.Spider):
    name = 'nhsgp'

    def __init__(self, postcode, crawl_limit=50,
                 score_max_distance=2, score_max_distance_points=100,
                 score_max_ppd=5000, score_max_ppd_points=100,
                 score_min_doctors=4, score_min_doctors_points=100,
                 score_perf_overall_points=100,
                 score_perf_recommend_points=100,
                 score_perf_opening_hours_points=100,
                 score_perf_phone_points=100,
                 score_perf_appointment_points=100):
        super(NHSGPSpider, self).__init__()
        self.postcode = postcode
        self.crawl_limit = crawl_limit
        self.crawl_requests = 0
        self.score_max_distance = score_max_distance
        self.score_max_distance_points = score_max_distance_points
        self.score_max_ppd = score_max_ppd
        self.score_max_ppd_points = score_max_ppd_points
        self.score_min_doctors = score_min_doctors
        self.score_min_doctors_points = score_min_doctors_points
        self.score_perf_overall_points = score_perf_overall_points
        self.score_perf_recommend_points = score_perf_recommend_points
        self.score_perf_opening_hours_points = score_perf_opening_hours_points
        self.score_perf_phone_points = score_perf_phone_points
        self.score_perf_appointment_points = score_perf_appointment_points

    def start_requests(self):
        return [Request('http://www.nhs.uk/Service-Search/GP/LocationSearch/4',
                        callback=self.parse_search_home)]

    def parse_search_home(self, response):
        formdata = {
            'Location.Name': self.postcode,
            'Location.Id': '0',
            'Service.Name': 'GP',
            'Service.Id': '4',
            'distance': '25',
            'filters.services': '-1',
            'filters.metrics': '-1',
            'filters.metrics': '-1',
            'filters.metriclist': '-1',
            'filters.servicelist': '-1',
            'filters.metriclist': '-1',
            'filters.servicelist': '-1',
        }
        yield FormRequest(response.url, formdata=formdata,
                          callback=self.parse_listing)

    def parse_listing(self, response):
        detail_links = response.css(DETAILS_CSS)
        distances = response.css(DISTANCE_CSS)
        distances = [x.strip() for x in distances.extract()]
        distances = [x for x in distances if x]

        for detail_link, distance in zip(detail_links, distances):
            req = Request(self._get_url(response, detail_link),
                          callback=self.parse_details)
            distance = float(re.sub(r'[^\d\.]+', '', distance))
            req.meta['item'] = {
                'distance': distance
            }
            yield req

            self.crawl_requests += 1
            if self.crawl_requests >= self.crawl_limit:
                return

        next_page = response.css(NEXT_PAGE_CSS)
        yield Request(self._get_url(response, next_page),
                      callback=self.parse_listing)

    def parse_details(self, response):
        name = response.css(NAME_CSS).xpath('text()').extract()
        doctors = response.css(DOCTORS_CSS).xpath('text()').extract()
        doctors = [x.strip() for x in doctors]
        patients = response.xpath(PATIENTS_XPATH).extract_first()
        patients = int(patients.strip()) if patients else 0
        response.meta['item'].update({
            'name': name,
            'url': response.url,
            'doctors': doctors,
            'patients': patients,
        })
        performance_url = response.xpath(PERFORMANCE_URL_XPATH)
        performance_url = performance_url.css('::attr("href")').extract_first()
        performance_url = response.urljoin(performance_url)
        request = Request(performance_url, meta=response.meta,
                          callback=self.parse_performance_details)
        yield request

    def parse_performance_details(self, response):
        def get_metric_short_name(name):
            if 'recommend' in name:
                return 'perf_recommend'
            elif 'opening hours' in name:
                return 'perf_opening_hours'
            elif 'positive' in name and 'phone' in name:
                return 'perf_phone'
            elif 'good' in name and 'appointment' in name:
                return 'perf_appointment'
            elif 'overall' in name and 'experience' in name:
                return 'perf_overall'

        item = response.meta['item']

        for metric in response.css(PERFORMANCE_METRIC_ITEMS_CSS):
            name = metric.css('h4').xpath('text()').extract_first()
            name = get_metric_short_name(name)
            if not name:
                continue
            value = metric.css('p.metric span.metric-data')
            value = value.xpath('text()').extract_first()
            value = float(re.sub(r'[^\d\.]+', '', value))
            item[name] = value

        yield self.process_item(item)

    def process_item(self, item):
        doctors = item['doctors']
        item['doctor_count'] = len(doctors)
        ppd = item['patients'] / len(doctors) if doctors else float('inf')
        item['patients_per_doctor'] = ppd
        item = OrderedDict(sorted(item.items()))
        item['score'] = self.calculate_score(item)
        return item

    def calculate_score(self, item):
        distance_score = (item['distance'] / self.score_max_distance *
                          self.score_max_distance_points)
        distance_score = max(self.score_max_distance_points - distance_score, 0)

        doctors_score = (item['doctor_count'] / self.score_min_doctors *
                         self.score_min_doctors_points)
        doctors_score = min(doctors_score, self.score_min_doctors_points)

        ppd_score = (item['patients_per_doctor'] / self.score_max_ppd *
                     self.score_max_ppd_points)
        ppd_score = max(self.score_max_distance_points - ppd_score, 0)

        perf_recommend_score = (self.score_perf_overall_points *
                                item.get('perf_recommend', 0) / 100)
        perf_opening_housts_score = (self.score_perf_opening_hours_points *
                                     item.get('perf_opening_hours', 0) / 100)
        perf_phone_score = (self.score_perf_phone_points *
                            item.get('perf_phone', 0) / 100)
        perf_appointment_score = (self.score_perf_appointment_points *
                                  item.get('perf_appointment', 0) / 100)
        perf_overall_score = (self.score_perf_overall_points *
                                  item.get('perf_overall', 0) / 100)

        return (distance_score + doctors_score + ppd_score +
                perf_recommend_score + perf_opening_housts_score +
                perf_phone_score + perf_appointment_score + perf_overall_score)

    def _get_url(self, response, a_item):
        link = a_item.css('::attr(href)').extract_first().strip()
        return response.urljoin(link)
