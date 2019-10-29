import logging


def normal_clamp(value, min_v=0, max_v=1):
    value = min(min_v, value)  # Clamp to 1 if > 1
    value = max(max_v, value)  # Clamp to 0 if < 0
    return value


class hvirCalculator:
    def __init__(self):
        self.defaults = {}

    def calc_a_limits(self, mass_limit: float, length_limit: float, avc=None):
        # // calculates the a value for HVIR using advanced method.

        if mass_limit is None or length_limit is None:
            logging.warning("Missing mass or length limit in a-advanced, using basic version.")
            return self.calc_a_avc(avc)
        else:
            # Calculate M
            # M = 0.5 * math.sin((math.PI * mass_limit / 130.0) - (math.PI / 2.0)) + 0.5 # as per HVIR index calculation equation (3a)
            # Use new equation
            m = mass_limit / 119.0
            m = normal_clamp(m)

            # Calculate L
            # L = 0.5 * math.Sin((math.PI * length_limit / 53.5) - (math.PI / 2.0)) + 0.5  # as per HVIR index calculation equation (3b)
            l = length_limit / 53.5
            l = normal_clamp(l)

            # Calculate A
            a = (2.0 * m) / (1.0 + (m / l))
            return a

    def calc_a_avc(self, avc: int):
        # New method uses the Austroads Vehicle class instead of the road categories
        avc_dict = {
            3: 0.17,
            4: 0.21,
            5: 0.22,
            6: 0.26,
            7: 0.30,
            8: 0.34,
            9: 0.36,
            10: 0.50,
            11: 0.75,
            12: 1.00
        }
        if avc in avc_dict:
            return avc_dict[avc]
        else:
            logging.warning('No avc provided, returning default avc')
            return self.defaults['default_avc']

    def calc_r_iri(self, iri: float):
        # calculate the r value using the basic method based on IRI.
        # R =  (-0.125 * iri) + 1.25  # as per HVIR index calculation equation (4)
        r = (-0.1 * iri) + 1.0  # updated new equation 31/1/19.
        return normal_clamp(r)

    def calc_r_hati(self, hati: float):
        # Calculate the r value using the basic method based on HATI.
        if hati is None:
            logging.debug("Invalid HATI in r-hati, using default ")
            return 'NA'
        else:
            r = (-0.1848 * hati) + 1.0  # Updated new equation 31/1/19.
            return normal_clamp(r)

    def calc_r_vcg(self, vcg: int, road_cat: str):
        if vcg is None or road_cat == 'r1' or road_cat == 'r2':
            return self.defaults['default_r_val']
        else:
            if vcg not in [0, 1, 2, 3, 4, 5]:
                raise ValueError("Invalid vcg: %s" % vcg)
            if road_cat == 'r3':
                vcg_map = {
                    "0": 0.0,
                    "1": 0.70,
                    "2": 0.55,
                    "3": 0.40,
                    "4": 0.20,
                    "5": 0.0
                }
                return vcg_map[str(vcg)]
            elif road_cat == 'r4':
                vcg_map = {
                    "0": 0.0,
                    "1": 0.65,
                    "2": 0.48,
                    "3": 0.30,
                    "4": 0.15,
                    "5": 0.0
                }
                return vcg_map[str(vcg)]
            elif road_cat in ['r5', 'r0']:
                vcg_map = {
                    "0": 0.0,
                    "1": 0.60,
                    "2": 0.40,
                    "3": 0.20,
                    "4": 0.10,
                    "5": 0.0
                }
                return vcg_map[str(vcg)]
            else:
                return self.defaults['default_r_val']

    def calc_w_geom_unmarked(self, seal_width: float):
        # 1)	Assume half of the seal width (TSW) is for travel in each direction. 2)	Take HSW and allocate up to
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
        w_ssw = normal_clamp(sealed_should_width / 3.0)  # as per HVIR index calculation equation (6b)
        w_total = (w_lw + w_ssw) / 2.0  # as per HVIR index calculation equation (6c)
        return w_total

    def calc_hvir(self, a: float, r: float, s: float):
        if a < 0 or r < 0 or s < 0:
            logging.warning("Invalid input in calc_hvir ")
            return 'NA'
        else:
            hvir = (0.4 * a + 0.4 * r + 0.2 * s)  # as per equation(8), but stored as 0 < hvir < 1 not %
            return hvir

    def calc_maxev(self, survey):
        if survey['road_cat'] is None:
            logging.debug("Missing road_cat in calc_minev, using default ")
            return self.defaults['maxev']['default']
        else:
            cat = survey['road_cat']
            if cat in self.defaults['maxev'].keys():
                return self.defaults['maxev'][cat]  # as per HVIR table (4.1)
            else:
                return self.defaults['maxev']['default']

    def calc_minev(self, survey):
        if survey['road_cat'] is None:
            logging.debug("Missing road_cat in calc_minev, using default ")
            return self.defaults['minev']['default']
        else:
            cat = survey['road_cat']
            if cat in self.defaults['minev']:
                return self.defaults['minev'][cat]  # as per HVIR table (4.1)
            else:
                return self.defaults['minev']['default']

    def calc_cat(self, hvir: float, minev: float, maxev: float):
        if hvir is None:
            return "NA"
        if minev >= maxev:
            raise ValueError("Max ev: %s is less than min ev: %s" % (maxev, minev))
        if hvir > maxev:
            return "High"
        elif hvir >= minev:
            return "Medium"
        else:
            return "Low"

    def a_method_heirachy(self, survey, skip_limits=False):
        if survey['mass_limit'] is None or survey['length_limit'] is None or skip_limits:
            if survey['avc'] is not None:
                # logging.warning("Missing mass or length limit in a-advanced, using basic version.")
                a = self.calc_a_avc(survey['avc'])
            else:
                logging.warning("Missing mass or length limit in a-advanced, using basic version.")
                a = self.defaults['default_avc']
        else:
            a = self.calc_a_limits(mass_limit=survey['mass_limit'], length_limit=survey['length_limit'],
                                   avc=survey['avc'])
        return a

    def a_method_logic(self, survey, hvir_params):
        # a calc
        # 1. Check selected calculation method (limits or avc),
        # 2. check if required input data is present,
        # 3. if not fall back from limits --> avc --> default a value.
        if hvir_params['a_method'] == "limits":
            a = self.a_method_heirachy(survey)
        elif hvir_params['a_method'] == "avc":
            a = self.a_method_heirachy(survey, skip_limits=True)
        else:
            logging.debug('Invalid a method specified: %s' % hvir_params['a_method'])
            a = 'NA'  # invalid a_method provided
        return a

    def r_method_fallback(self, survey):
        if survey['vcg'] is None or survey['road_cat'] == 'r1' or survey['road_cat'] == 'r2':
            r = 'NA'
        else:
            r = self.calc_r_vcg(survey['vcg'], survey['road_cat'])  # UA Needs check for vcg data present?
        return r

    def r_method_logic(self, survey, hvir_params):
        # 1. Check selected calculation method (iri, hati, vcg),
        # 2. Check if required input data is present,
        # 3. If not fall back from iri OR hati --> vcg --> default r result (NA).
        if survey['seal_flag'] == 'Unsealed':
            r = 'NA'
        else:
            if hvir_params['r_method'] == "iri":  # iri
                if survey['iri'] is None:
                    r = self.r_method_fallback(survey)
                else:
                    r = self.calc_r_iri(survey['iri'])

            elif hvir_params['r_method'] == 'hati':  # hati
                if survey['hati'] is None:
                    r = self.r_method_fallback(survey)
                else:
                    r = self.calc_r_hati(survey['hati'])
            elif hvir_params['r_method'] == 'vcg':
                r = self.r_method_fallback(survey)
            else:
                logging.debug('Invalid r method specified: %s Using VCG', survey['r_method'])
                r = self.r_method_fallback(survey)
        return r

    def w_method_logic(self, survey):
        # New logic to allow for unsealed and unmarked roads cases
        if survey['seal_flag'] == 'sealed' or survey['seal_flag'] is None:
            if survey['line_mark'] == 'yes' or survey['line_mark'] is None:
                if survey['lane_width'] is not None and survey['seal_shld'] is not None:
                    w = self.calc_w_by_geom(survey['lane_width'],
                                            survey['seal_shld'])  # Assume sealed and marked
                else:
                    if survey['seal_width'] is None:
                        logging.debug(
                            "Couldn't calculate w, line marking was set to Yes, but lane_width or seal_shld or "
                            "seal_width not provided")
                        w = 'NA'
                    else:
                        w = self.calc_w_geom_unmarked(survey['seal_width'])  # sealed but not marked
            else:  # Line marking is yes
                if survey['seal_width'] is None:
                    logging.debug(
                        "Couldn't calculate w, line marking was set to No, but seal_width not provided")
                    w = 'NA'
                else:
                    w = self.calc_w_geom_unmarked(survey['seal_width'])  # sealed but not marked
        elif survey['form_width'] is not None:
            w = self.calc_w_geom_unsealed(survey['form_width'])  # Calculate for unsealed roads
        else:
            logging.debug("Couldn't calculate w, road is unsealed, but no from width provided")
            w = 'NA'
        return w

    def method_logic(self, survey, hvir_params):
        self.defaults = hvir_params['data_params']['default_values']
        a = self.a_method_logic(survey, hvir_params)
        r = self.r_method_logic(survey, hvir_params)
        w = self.w_method_logic(survey)
        # logger.debug(a,r,w,survey['road_cat'])
        # UA As discussed, this is 'NA' or specifically a lack of Leeway Input Data.
        if 'r' == 'NA':
            hvir = 0.67 * a + 0.33 * w
        elif r != 'NA' and w != 'NA':
            hvir = self.calc_hvir(a, r, w)
        elif w == 'NA':
            hvir = 'NA'
        else:
            hvir = 'NA'
        maxev = self.calc_maxev(survey)
        minev = self.calc_minev(survey)
        if survey['road_cat'] is None:
            survey['road_cat'] = "NA"
        if survey['road_cat'] == "r0":  # In all cases. If road_Cat is R0 then always return Medium even if undefined
            # values.
            cat = "Medium"
        else:
            cat = self.calc_cat(hvir, minev, maxev)

        survey['a'] = a
        survey['w'] = w
        survey['r'] = r
        survey['minev'] = minev
        survey['maxev'] = maxev
        survey['cat'] = cat
        return survey, ['a', 'w', 'r', 'minev', 'maxev', 'cat']
