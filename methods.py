import logging


def normal_clamp(value, min_v=0, max_v=1):
    value = max(min_v, value)  # Clamp to 1 if > 1
    value = min(max_v, value)  # Clamp to 0 if < 0
    return value


class HvirCalculator:
    def __init__(self):
        self.defaults = {}

    def calc_a_limits(self, mass_limit: float, length_limit: float, avc=None):
        # calculates the a value for HVIR using advanced method.
        if mass_limit is None or length_limit is None:
            logging.warning("Missing mass or length limit in a-advanced, using basic version.")
            return self.calc_a_avc(avc)
        else:
            # Calculate M
            m = mass_limit / 122.5
            m = normal_clamp(m)

            # Calculate L
            l = length_limit / 53.5
            l = normal_clamp(l)

            # Calculate A
            a = (2.0 * m) / (1.0 + (m / l))
            return a, 'L'

    def calc_a_avc(self, avc: int):
        avc_dict = {
            3: 0.17,
            4: 0.21,
            5: 0.24,
            6: 0.25,
            7: 0.29,
            8: 0.34,
            9: 0.35,
            10: 0.50,
            11: 0.75,
            12: 1.00
        }
        if avc in avc_dict:
            return avc_dict[avc], 'A'
        else:
            logging.warning('No avc provided, returning default avc')
            return self.defaults['default_avc'], 'D'

    def calc_r_iri(self, iri: float):
        # calculate the r value using the basic method based on IRI.
        r =  0.0075*iri**3-0.107*iri**2+0.277*iri+0.8014
        return normal_clamp(r), 'I'

    def calc_r_hati(self, hati: float):
        # Calculate the r value using the basic method based on HATI.
        if hati is None:
            logging.debug("Invalid HATI in r-hati, using default ")
            return 'NA','N'
        else:
            r = (-0.1848 * hati) + 1.0
            return normal_clamp(r),'H'

    def calc_r_vcg(self, vcg: int, road_cat: str):
        if vcg is None or road_cat == 'r1' or road_cat == 'r2':
            return self.defaults['default_r_val'],'D'
        else:
            if vcg not in [0, 1, 2, 3, 4, 5]:
                raise ValueError("Invalid vcg: %s" % vcg)
            if road_cat == 'r3':
                vcg_map = {
                    "0": 0.0,
                    "1": 0.92,
                    "2": 0.73,
                    "3": 0.54,
                    "4": 0.27,
                    "5": 0.0
                }
                return vcg_map[str(vcg)], 'V'
            elif road_cat == 'r4':
                vcg_map = {
                    "0": 0.0,
                    "1": 0.78,
                    "2": 0.62,
                    "3": 0.45,
                    "4": 0.23,
                    "5": 0.0
                }
                return vcg_map[str(vcg)], 'V'
            elif road_cat in ['r5', 'r0']:
                vcg_map = {
                    "0": 0.0,
                    "1": 0.74,
                    "2": 0.60,
                    "3": 0.45,
                    "4": 0.23,
                    "5": 0.0
                }
                return vcg_map[str(vcg)], 'V'
            else:
                return self.defaults['default_r_val'], 'D'

    def calc_w_geom_unmarked(self, seal_width: float):
        # 1) Assume half of the seal width (TSW) is for travel in each direction. 2)	Take HSW and allocate up to
        # 2.9 m as the lane width. 3)	If there is any HSW remaining, divide it equally between additional lane width
        # and sealed shoulder width, limiting lane width to a maximum of 5.8 m. 4)	Add any additional HSW to the
        # sealed shoulder width. 5)	Once values for Lane Width and Sealed Shoulder Width have been finalised,
        # calculate Safety with the By Geometry method.
        half_seal_width = seal_width / 2.0
        if half_seal_width <= 2.9:
            lane_width = half_seal_width
            sealed_shoulder_width = 0.0
        elif half_seal_width <= 5.8:
            lane_width = 2.9 + ((half_seal_width - 2.9) / 2.0)
            sealed_shoulder_width = (half_seal_width - 2.9) / 2.0
        else:
            lane_width = 5.8
            sealed_shoulder_width = half_seal_width - 5.8
        return self.calc_w_by_geom(lane_width, sealed_shoulder_width)

    def calc_w_geom_unsealed(self, form_width: float):
        half_form_width = form_width / 2.0
        if half_form_width <= 2.9:
            lane_width = half_form_width
            sealed_shoulder_width = 0.0
        elif half_form_width <= 5.8:
            lane_width = 2.9 + ((half_form_width - 2.9) / 2.0)
            sealed_shoulder_width = (half_form_width - 2.9) / 2.0
        else:
            lane_width = 5.8
            sealed_shoulder_width = half_form_width - 5.8
        return self.calc_w_by_geom(lane_width, sealed_shoulder_width)

    def calc_w_by_geom(self, lane_width: float, sealed_should_width: float):
        w_lw = normal_clamp(lane_width / 5.8)
        w_ssw = normal_clamp(sealed_should_width / 3.0)
        w_total = (w_lw + w_ssw) / 2.0
        return w_total

    def calc_hvir(self, a, r, w, seal_flag):
            if seal_flag == 'unsealed':
                if a != 'NA' and w != 'NA':
                    return 0.67*a + 0.33*w
                else:
                    return 'NA'
            else:
                if a != 'NA' and r != 'NA' and w != 'NA':
                    return 0.4*a + 0.4*r + 0.2*w
                else:
                    return 'NA'

    def calc_maxev(self, survey):
        if survey['road_cat'] is None:
            logging.debug("Missing road_cat in calc_minev, using default ")
            return self.defaults['maxev']['default']
        else:
            cat = survey['road_cat']
            if cat in self.defaults['maxev'].keys():
                return self.defaults['maxev'][cat]
            else:
                return self.defaults['maxev']['default']

    def calc_minev(self, survey):
        if survey['road_cat'] is None:
            logging.debug("Missing road_cat in calc_minev, using default ")
            return self.defaults['minev']['default']
        else:
            cat = survey['road_cat']
            if cat in self.defaults['minev']:
                return self.defaults['minev'][cat]
            else:
                return self.defaults['minev']['default']

    def calc_cat(self, hvir: float, minev: float, maxev: float):
        if hvir == "NA":
            return "NA"
        if minev >= maxev:
            raise ValueError("Max ev: %s is less than min ev: %s" % (maxev, minev))
        if hvir > maxev:
            return "High"
        elif hvir > minev:
            return "Medium"
        elif hvir <= minev:
            return 'Low'
        else:
            return "Low"

    def a_method_heirachy(self, survey, skip_limits=False):
        if survey['mass_lim'] is None or survey['len_lim'] is None or skip_limits:
            if survey['avc'] is not None:
                logging.debug("Missing mass or length limit in a-advanced, using basic version.")
                a, methods  = self.calc_a_avc(survey['avc'])
            else:
                logging.warning("Missing mass or length limit in a-advanced, using basic version.")
                a, methods = self.defaults['default_avc'], 'D'
        else:
            #logging.debug('using limits')
            a, methods = self.calc_a_limits(mass_limit=survey['mass_lim'], length_limit=survey['len_lim'],
                                   avc=survey['avc'])

        return a, methods

    def a_method_logic(self, survey, hvir_params):
        # a calc
        # 1. Check selected calculation method (limits or avc),
        # 2. check if required input data is present,
        # 3. if not fall back from limits --> avc --> default a value.
        if hvir_params['a_method'] == "limits":
            a, methods = self.a_method_heirachy(survey)
        elif hvir_params['a_method'] == "avc":
            logging.debug('using avc')
            a, methods = self.a_method_heirachy(survey, skip_limits=True)
        else:
            logging.debug('Invalid a method specified: %s' % hvir_params['a_method'])
            a = 'NA'  # invalid a_method provided
        return a, methods

    def r_method_fallback(self, survey):
        if survey['iri'] is None:
            if survey['vcg'] is None or survey['road_cat'] == 'r1' or survey['road_cat'] == 'r2':
                r, methods = 'NA', 'N'
            else:
                r, methods = self.calc_r_vcg(survey['vcg'], survey['road_cat'])
            return r, methods
        else:
            return self.calc_r_iri(survey['iri'])

    def r_method_logic(self, survey, hvir_params):
        # 1. Check selected calculation method (iri, hati, vcg),
        # 2. Check if required input data is present,
        # 3. If not fall back from iri OR hati --> vcg --> default r result (NA).
        if survey['seal_flag'] == 'unsealed':
            r, methods = 'NA', 'N'
        else:
            if hvir_params['r_method'] == "iri":  # iri
                if survey['iri'] is None:
                    r, methods = self.r_method_fallback(survey)
                else:
                    r, methods = self.calc_r_iri(survey['iri'])

            elif hvir_params['r_method'] == 'hati':  # hati
                if survey['hati'] is None:
                    r, methods = self.r_method_fallback(survey)
                else:
                    r, methods = self.calc_r_hati(survey['hati'])
            elif hvir_params['r_method'] == 'vcg':
                r, methods = self.r_method_fallback(survey)
            else:
                logging.debug('Invalid r method specified: %s Using VCG', survey['r_method'])
                r, methods = self.r_method_fallback(survey)
        return r, methods

    def w_method_logic(self, survey):
        # Logic to allow for unsealed and unmarked roads cases
        if survey['seal_flag'] == 'sealed' or survey['seal_flag'] is None:
            if survey['line_mark'] == 'yes' or survey['line_mark'] is None:
                if survey['lane_width'] is not None and survey['seal_shld'] is not None:
                    w, methods= self.calc_w_by_geom(survey['lane_width'],
                                            survey['seal_shld']),'M' # Assume sealed and marked
                else:
                    if survey['seal_width'] is None:
                        logging.debug(
                            "Couldn't calculate w, line marking was set to Yes, but lane_width or seal_shld or "
                            "seal_width not provided")
                        w, methods = 'NA', 'N'
                    else:
                        w, methods = self.calc_w_geom_unmarked(survey['seal_width']),'S'  # sealed but not marked
            else:
                if survey['seal_width'] is None:
                    logging.debug(
                        "Couldn't calculate w, line marking was set to No, but seal_width not provided")
                    w, methods = 'NA', 'N'
                else:
                    w, methods = self.calc_w_geom_unmarked(survey['seal_width']),'S'  # Sealed but not marked
        elif survey['form_width'] is not None:
            w, methods = self.calc_w_geom_unsealed(survey['form_width']), 'S'  # Calculate for unsealed roads
        else:

            logging.debug("Couldn't calculate w, road is unsealed, but no from width provided")
            w, methods = 'NA', 'N'
        return w, methods

    def method_logic(self, survey, hvir_params):
        self.defaults = hvir_params['data_params']['default_values']
        a,a_method = self.a_method_logic(survey, hvir_params)
        r,r_method = self.r_method_logic(survey, hvir_params)
        w,w_method = self.w_method_logic(survey)
        hvir = self.calc_hvir(a,r,w,survey['seal_flag'])
        maxev = self.calc_maxev(survey)
        minev = self.calc_minev(survey)

        if survey['road_cat'] == "r0":  # In all cases. If road_Cat is R0 then always return Medium even if undefined
            cat = "Medium"
        else:
            cat = self.calc_cat(hvir, minev, maxev)

        survey['a'] = a
        survey['w'] = w
        survey['r'] = r
        survey['hvir'] = hvir
        survey['minev'] = minev
        survey['maxev'] = maxev
        survey['cat'] = cat
        survey['calc-methods'] = "%s-%s-%s" % (a_method,r_method,w_method)
        return survey, ['a', 'w', 'r', 'hvir', 'minev', 'maxev', 'cat','calc-methods']
