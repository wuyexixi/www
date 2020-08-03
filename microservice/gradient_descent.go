package microservice

import "math"

//Regression func
func Regression(x_values []float64, y_values []float64, z_values []float64, epochs int, learning_rate float64) (float64, float64, float64, float64) {
	if len(x_values) == 0 || len(y_values) == 0 || len(z_values) == 0 {
		panic("Input arrays must not be empty.")
	}

	var a_current float64 = 0
	var b_current float64 = 0
	var alpha_current float64 = 0
	var beta_current float64 = 0

	for i := 0; i < epochs; i++ {
		a_current, b_current, alpha_current, beta_current = step(a_current, b_current, alpha_current, beta_current, x_values, y_values, z_values, learning_rate)
	}

	return a_current, b_current, alpha_current, beta_current
}

//Step fuc
func step(a_current float64, b_current float64, alpha_current float64, beta_current float64, x_values []float64, y_values []float64, z_values []float64, learning_rate float64) (float64, float64, float64, float64) {
	var a_gradient float64 = 0
	var b_gradient float64 = 0
	var alpha_gradient float64 = 0
	var beta_gradient float64 = 0

	var length = len(z_values)

	for i := 0; i < length; i++ {
		var two_over_n = float64(2) / float64(length)
		a_gradient += -two_over_n * math.Exp((alpha_current*x_values[i])+(beta_current*y_values[i])) * (z_values[i] - (a_current*math.Exp((alpha_current*x_values[i])+(beta_current*y_values[i])) + b_current))
		b_gradient += -two_over_n * (z_values[i] - (a_current*math.Exp((alpha_current*x_values[i])+(beta_current*y_values[i])) + b_current))
		alpha_gradient += -two_over_n * x_values[i] * a_current * math.Exp((alpha_current*x_values[i])+(beta_current*y_values[i])) * (z_values[i] - (a_current*math.Exp((alpha_current*x_values[i])+(beta_current*y_values[i])) + b_current))
		beta_gradient += -two_over_n * y_values[i] * a_current * math.Exp((alpha_current*x_values[i])+(beta_current*y_values[i])) * (z_values[i] - (a_current*math.Exp((alpha_current*x_values[i])+(beta_current*y_values[i])) + b_current))

	}

	var new_a = a_current - (learning_rate * a_gradient)
	var new_b = b_current - (learning_rate * b_gradient)
	var new_alpha = alpha_current - (learning_rate * alpha_gradient)
	var new_beta = beta_current - (learning_rate * beta_gradient)

	return new_a, new_b, new_alpha, new_beta
}
