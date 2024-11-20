package xugu

type TIMESTAMP struct {
	year     int
	month    int
	day      int
	hour     int
	minute   int
	second   int
	fraction int // 毫秒
}

// 是否是闰年
func IsLeapYear(year int) bool {
	return (year%4 == 0 && year%100 != 0) || (year%400 == 0)
}

// 月份到天数的映射
var mtod = [2][13]int{
	{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365}, // 非闰年
	{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366}, // 闰年
}

func dt2dtm(t int64) TIMESTAMP {
	var (
		y, m, d, s int
		mm, nn     int
		wday       int
		ms         int
		rn_num     int
	)

	if t >= 0 { // 1970年以后
		ms = int(t % 1000)
		t /= 1000
		s = int(t % 86400)
		d = int(t / 86400)
		wday = (d + 4) % 7
		mm = d / 146097
		nn = d % 146097
		y = 1970 + 400*mm
		mm = nn / 36524
		nn = nn % 36524
		y += 100 * mm
		mm = nn / 1461
		nn = nn % 1461
		y += 4 * mm
		if nn > 1096 {
			y += 3
		}
		if nn > 730 && nn <= 1096 {
			y += 2
		}
		if nn > 365 && nn <= 730 {
			y++
		}
		if nn == 0 {
			y--
		}
		rn_num = (y-1)/4 - (y-1)/100 + (y-1)/400
		rn_num -= 477
		d = d - 365*(y-1970) - rn_num
	} else { // 1970年以前
		ms = int(t % 1000)
		t /= 1000
		if ms != 0 {
			ms += 1000
			t--
		}
		s = int(t % 86400)
		d = int(t / 86400)
		if s != 0 {
			s += 86400
			d--
		}
		wday = (d + 4) % 7
		if wday < 0 {
			wday += 7
		}
		mm = d / 146097
		nn = d % 146097
		y = 1969 + 400*mm
		mm = nn / 36524
		nn = nn % 36524
		y += 100 * mm
		mm = nn / 1461
		nn = nn % 1461
		y += 4 * mm
		if nn < -1096 {
			y -= 3
		}
		if nn < -731 && nn >= -1096 {
			y -= 2
		}
		if nn < -365 && nn >= -731 {
			y--
		}
		if nn == 0 {
			y++
		}
		rn_num = y/4 - y/100 + y/400
		rn_num -= 477
		d = d - 365*(y+1-1970) - rn_num
		if IsLeapYear(y) {
			d += 366
		} else {
			d += 365
		}
	}

	if d < 0 {
		y--
		if IsLeapYear(y) {
			d += 366
		} else {
			d += 365
		}
	}

	d++
	if IsLeapYear(y) {
		if d > 366 {
			d -= 366
			y++
		}
	} else if d > 365 {
		d -= 365
		y++
	}

	if IsLeapYear(y) {
		for m = 0; m <= 11; m++ {
			if d > mtod[1][m] && d <= mtod[1][m+1] {
				d -= mtod[1][m]
				break
			}
		}
	} else {
		for m = 0; m <= 11; m++ {
			if d > mtod[0][m] && d <= mtod[0][m+1] {
				d -= mtod[0][m]
				break
			}
		}
	}

	return TIMESTAMP{
		year:     y,
		month:    m + 1,
		day:      d,
		hour:     s / 3600,
		minute:   (s % 3600) / 60,
		second:   s % 60,
		fraction: ms,
	}
}
