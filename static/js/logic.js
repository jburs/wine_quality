// Add console.log to check to see if our code is working.
console.log("working");


$(document).ready(function() {
    $('#fullpage').fullpage({
    navigation: true,
    navigationPosition: 'right',
    navigationTooltips: ['section1', 'section2','section3','section4',],
    showActiveTooltip: true,
    slidesNavigation: true,
    slidesNavPosition: 'bottom',
    controlArrows:false,
    });
});

