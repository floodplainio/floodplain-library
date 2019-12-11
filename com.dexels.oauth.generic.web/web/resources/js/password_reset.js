$(function() {
	$('#login-forgot').submit(function(event) {
		hideAlert();
		event.preventDefault();
		var username = $('#forgot-username').val();

		if (shouldPrevent(username)) {
			displayAlert('danger', '', "Er is geen e-mailadres ingevoerd");
			return undefined;
		}

		var parameters = {
			username : encodeURIComponent(username),
		};
		request('/oauth/forgot', parameters, 'forgot-button', '.content');
	});
});