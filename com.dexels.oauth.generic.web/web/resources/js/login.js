$.getJSON("/oauth/loginstatus", function(data) {
	$(function() {
		handleLoginStatus(data);
	});
});

$(function() {
    if (!checkCookie()) {
        displayAlert('danger', 'Error', 'Uw browser staat momenteel ingesteld om cookies te blokkeren. U kunt pas inloggen wanneer u cookies toestaat.');
        return;
    }
 
    $( '#login-form').submit(function(event) {
    	hideAlert();
        var username = $('#login-username').val(); 
        var password = $('#login-password').val();
        
//        var keepSession = $('#login-keepSession').is(":checked")
        
	if(shouldPrevent(username)) {
	    displayAlert('danger', '', "Er is geen e-mailadres ingevoerd");
	    event.preventDefault();
	} else if(shouldPrevent(password)) {
            displayAlert('danger', '', "Er is geen wachtwoord ingevoerd");
            event.preventDefault();
        } else if (!email_regex.test(username)) {
            displayAlert( 'danger', '', "Ongeldig e-mailadres ingevoerd" );            
            event.preventDefault();
	}
    });
    

	$(document).on('click', '#passwordResetLink', function(event) {
		event.preventDefault();
		var type = window.location.hash.substr(1);
		window.location.href = 'password_reset.html#' + type;
		return true;
	});
    
	var regdiv = $('<span/>')
	regdiv.html("Nog geen account? <a href='register.html' id='startregistration' class='union-text-color'>Registreren </a>");
	$('#registrationdiv').append(regdiv);
	$('#registrationdiv').show();
	$('a.union-text-color').css('color','#0275d8')
	
	$(document).on('click', '#startregistration', function(event) {
		event.preventDefault();
		var type = window.location.hash.substr(1);
		window.location.href = 'register.html#' + type;
		return true;
	});
});
