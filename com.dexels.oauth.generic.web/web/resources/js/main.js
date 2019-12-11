

$(function() {
	document.title = 'Login met je account';
	
	document.documentElement.style.setProperty('--heightFooter', '40px');

	$('.account-placeholder').text("Account");
	$('#login-username').attr('placeholder','E-mailadres');
			
	// construct footer
	$('#union-footer-copyright').html('<span class="col text-center copyright">© 2019</span>');
	$('#union-footer-apps').remove();	
	
	$(document).on('click', '#loginLink', function(event) {
		event.preventDefault();
		var type = window.location.hash.substr(1);
		window.location.href = 'login.html#' + type;
		return true;
	});
	
});
